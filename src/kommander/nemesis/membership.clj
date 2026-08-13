(ns kommander.nemesis.membership
  "Membership nemesis: takes a node out of the voting roster and puts it back.

  ## How membership is actually changed

  Not through an API. Kommander exposes `JoinCluster` / `LeaveCluster` on
  `IRaft` and nothing over HTTP — `/cluster/membership` is a read. The harness
  drives both from the process lifecycle:

  * `--join-existing` makes a starting node ask the seeds in
    `--initial-cluster` for admission. It is admitted as a Learner, backfilled,
    and promoted to Voter once caught up.
  * `--graceful-leave-on-shutdown` makes the node commit `RemoveMember(self)`
    from `ClusterService.StopAsync`.

  So a leave is SIGTERM-and-wait and a join is a restart with an extra flag.
  Both are slow — tens of seconds — which is why this fault runs on a longer
  interval than the others.

  ## Two ways a nemesis like this silently does nothing

  Both produce a *clean* verdict while changing no membership at all, so both
  are guarded here and the outcome is recorded in the history rather than
  assumed.

  1. **Firing before the cluster formed.** With no elected system-partition
     leader there is nothing to commit a RemoveMember, so the node just dies and
     the fault degenerates into a slow `kill`. Hence the `cluster-formed?` gate.
  2. **SIGKILLing before the leave finished.** .NET's default host shutdown
     budget is 30 s and the leave spends up to 20 s of it; a shorter grace
     period kills the node mid-leave every time and the roster never moves. See
     `kommander.db/graceful-stop-timeout-s`.

  ## Why only one node leaves at a time

  Five voters tolerate two failures. This nemesis removes exactly one, taking
  the cluster to four voters (quorum 3, one failure tolerated), and puts it back
  before removing another. Combined with `partition` or `kill` that is already
  enough to reach the edge; removing two would make quorum loss the *expected*
  outcome and every workload would grind to failure for reasons no checker could
  distinguish from a real bug.

  Kommander refuses a removal that would leave too few voters
  (`InsufficientVoters`), but the useful bound is well above that floor, so it
  has to come from here.

  `cluster-formed?` also means that when this fault is combined with `partition`
  or `kill`, most membership operations will decline — a cluster missing a node
  is not fully formed. That is the conservative choice, and it makes the
  combination weaker than it looks: read the `:leave` values before concluding a
  combined run exercised membership at all.

  ## Why a departing node is wiped

  A node that leaves and comes back is a *new* member. Its WAL is deleted so it
  rejoins as a Learner and is backfilled from scratch — which also means this
  nemesis exercises the catch-up path the log-append workload checks. Rejoining
  while carrying a log from a membership epoch it no longer belongs to is a
  different scenario, and one Kommander never claims to support; testing it here
  by accident would produce findings this suite could not defend."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]]
            [kommander.db :as kdb]))

(defn- act!
  "Runs `f` on `node` in that node's SSH context, returning its result."
  [test node f]
  (-> (c/on-nodes test [node] (fn [test node] (f test node)))
      (get node)))

(defn nemesis
  "Removes one node from the roster and adds it back.

  The node currently out is held in an atom here rather than chosen by the
  generator on purpose: the generator is replayed and its choices must stay
  reproducible, whereas the nemesis is a single stateful actor and the only
  thing that knows what it actually did."
  [_opts]
  (let [out (atom nil)]
    (reify n/Nemesis
      (setup! [this _test] this)

      (invoke! [_ test op]
        (case (:f op)
          :leave
          (cond
            @out
            (assoc op :value [:already-out @out])

            (not (kdb/cluster-formed? test))
            (assoc op :value :cluster-not-formed)

            :else
            (let [node    (rand-nth (vec (:nodes test)))
                  ;; A survivor's view is the only trustworthy one: the
                  ;; departing node is about to be stopped and wiped.
                  witness (first (remove #{node} (:nodes test)))
                  before  (:members (kdb/membership witness))]
              (info "membership: removing" node)
              (let [stopped (act! test node kdb/leave!)
                    after   (:members (kdb/membership witness))
                    shrank? (boolean (and before after
                                          (< (count after) (count before))))]
                (reset! out node)
                (assoc op :value {:node    node
                                  :stop    stopped
                                  :roster  [(count before) (count after)]
                                  ;; The whole point of the operation. A false
                                  ;; here means the node went away without
                                  ;; leaving the roster — worth seeing in the
                                  ;; history rather than inferring later.
                                  :removed shrank?}))))

          :join
          (if-let [node @out]
            (do (info "membership: rejoining" node)
                (let [res (try
                            (act! test node kdb/join!)
                            (catch Exception e
                              ;; A join that fails leaves the node out of the
                              ;; roster; say so rather than clearing `out` and
                              ;; letting the next :leave drop a second node.
                              (warn e "membership: rejoin of" node "failed")
                              :join-failed))]
                  (when (= :joined res)
                    (reset! out nil))
                  (assoc op :value {:node node :join res})))
            (assoc op :value :nobody-out))))

      (teardown! [_ _test])

      n/Reflection
      (fs [_] #{:leave :join}))))

(defn package
  "A nemesis package shaped like the ones jepsen.nemesis.combined returns, so it
  can be handed to `nc/compose-packages`. Returns the no-op package unless
  :membership is in :faults.

  The interval is deliberately independent of the other faults': a leave waits
  for the departing node to commit its removal and a join waits for a Learner to
  catch up, so at the 15 s default the two operations would overlap and the
  roster would spend the whole test mid-change."
  [opts]
  (if-not (some #{:membership} (:faults opts))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    (let [interval (:membership-interval opts 30)]
      {:generator       (->> [{:type :info, :f :leave}
                              {:type :info, :f :join}]
                             cycle
                             (gen/stagger interval))
       ;; Whatever is out at the end comes back, so the final read runs against
       ;; a whole cluster. Without this the log-append checker would be handed
       ;; four replicas instead of five and would quietly check less.
       :final-generator {:type :info, :f :join}
       :nemesis         (nemesis opts)
       :perf            #{{:name  "membership"
                           :start #{:leave}
                           :stop  #{:join}
                           :color "#B8E9A0"}}})))
