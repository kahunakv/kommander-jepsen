(ns kommander.db
  "Installs, starts, stops, kills and pauses the Kommander Jepsen harness on
  each node.

  The harness is a .NET application. Rather than install a .NET runtime on
  every Jepsen node, we ship a *self-contained* publish as a tarball built on
  the host (see scripts/build-tarball.sh) and upload it. The tarball's root must
  contain a `KommanderJepsen.Harness` executable."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [kommander.client :as kc]
            [slingshot.slingshot :refer [try+]]))

(def dir     "/opt/kommander")
(def binary  (str dir "/KommanderJepsen.Harness"))
(def logfile (str dir "/kommander.log"))
(def pidfile (str dir "/kommander.pid"))
(def wal-dir (str dir "/wal"))

(def raft-port
  "Inter-node Raft traffic (h2c gRPC). Separate from the client API port so a
  partition of this port breaks replication while client connections survive."
  8082)

(defn node-id
  "Kommander wants a small integer node id. Jepsen node names are conventionally
  n1..n5, so derive the id from the trailing digits, falling back to position in
  the node list."
  [test node]
  (if-let [n (re-find #"\d+$" (name node))]
    (Long/parseLong n)
    (inc (.indexOf ^java.util.List (vec (:nodes test)) node))))

(defn peers
  "The --initial-cluster seed list: every node *except* this one, as host:port.
  Kommander's own docker-compose excludes self, so we do too."
  [test node]
  (->> (:nodes test)
       (remove #(= % node))
       (map #(str (name %) ":" raft-port))))

(defn membership-faults?
  "Is the membership nemesis enabled? Governs whether nodes start with
  --graceful-leave-on-shutdown, which is a *start* flag: a node booted without
  it will never shrink the roster no matter how politely it is asked to stop."
  [test]
  (boolean (some #{:membership} (:faults test))))

(defn start-args
  "`join?` starts the node with --join-existing, which makes it ask the seeds in
  --initial-cluster for admission (as a Learner, promoted once caught up)
  instead of booting as a static-discovery member. Only meaningful for a node
  that has *left* the roster."
  ([test node] (start-args test node false))
  ([test node join?]
   (concat
     [:--raft-nodename (name node)
      :--raft-nodeid   (node-id test node)
      :--raft-host     (name node)
      :--raft-port     raft-port
      :--http-port     kc/http-port
      :--initial-cluster-partitions (:partitions test 4)
      :--wal-path      wal-dir
      :--wal-revision  :v1
      :--transport     (name (:transport test :grpc))]
     [:--initial-cluster] (peers test node)
     ;; Durability knob: without fsync a node that is SIGKILLed may lose
     ;; acknowledged writes, which is a legitimate finding only if you are
     ;; testing that configuration on purpose. Default here is to fsync.
     (when (:disable-wal-sync-writes test)
       [:--disable-wal-sync-writes true])
     ;; Safe to set unconditionally alongside the other faults: it only fires
     ;; from StopAsync, which a SIGKILL never reaches. The :kill fault therefore
     ;; still models a crash, not a polite departure.
     (when (membership-faults? test)
       [:--graceful-leave-on-shutdown true])
     (when join?
       [:--join-existing true]))))

(defn start!
  ([test node] (start! test node false))
  ([test node join?]
   (c/su
     (cu/start-daemon!
       {:chdir   dir
        :logfile logfile
        :pidfile pidfile
        :env     {:DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS 1}}
       binary
       (start-args test node join?)))))

(def process-pattern
  "pgrep -f pattern matching the harness process — and deliberately NOT matching
  the kill pipeline that carries it.

  `grepkill!` expands to `pgrep -f <pattern> | xargs kill -SIG`, so the
  pipeline's own command line contains the pattern verbatim. With a plain
  \"KommanderJepsen.Harness\" the pipeline matches itself: `kill -kill` SIGKILLs
  its own xargs and exits 137, and `kill -stop` would SIGSTOP the pipeline and
  hang forever.

  The bracket makes the two strings differ while matching the same process: the
  regex `KommanderJepsen[.]Harness` matches the real process's
  `KommanderJepsen.Harness`, but the literal text in the pipeline's own command
  line does not match it."
  "KommanderJepsen[.]Harness")

(defn kill-stragglers!
  "SIGKILLs any harness left running. Tolerates failure: the nemesis cheerfully
  kills a node that is already dead, and that must never abort a test with a
  complete history waiting to be analyzed."
  []
  (try+
    (c/su (cu/grepkill! :kill process-pattern))
    (catch Object _ nil)))

(defn stop!
  [_test _node]
  (c/su (cu/stop-daemon! binary pidfile))
  (kill-stragglers!))

(def graceful-stop-timeout-s
  "Seconds to wait for a SIGTERMed node to exit before SIGKILLing it.

  Must clear the *host's* shutdown budget, not just the leave's. .NET's default
  HostOptions.ShutdownTimeout is 30 s and applies to every hosted service;
  ClusterService.StopAsync spends up to 20 s of it inside LeaveCluster. A
  SIGKILL before that budget is exhausted truncates the leave and the roster
  never shrinks — which looks exactly like a server-side membership bug and is
  not one."
  45)

(defn graceful-stop!
  "SIGTERMs the harness and waits for it to exit, so its shutdown hook can run.

  `jepsen.control.util/stop-daemon!` cannot be used here: it sends SIGKILL
  outright, which skips StopAsync entirely — the very hook that commits
  RemoveMember. The kill -9 at the end is only a backstop for a node that hangs
  past the timeout; reaching it means the leave did not commit.

  Returns :left if the process exited on its own, :killed if it had to be
  SIGKILLed, and :not-running if there was nothing to stop."
  [_test _node]
  (c/su
    (keyword
      (c/exec :bash :-c
        (str "pid=$(cat " pidfile " 2>/dev/null); "
             "if [ -z \"$pid\" ] || ! kill -0 \"$pid\" 2>/dev/null; then "
             "  echo not-running; exit 0; fi; "
             "kill -TERM \"$pid\" 2>/dev/null; "
             "for _ in $(seq 1 " graceful-stop-timeout-s "); do "
             "  kill -0 \"$pid\" 2>/dev/null || break; sleep 1; done; "
             "if kill -0 \"$pid\" 2>/dev/null; then "
             "  kill -9 \"$pid\" 2>/dev/null; echo killed; "
             "else echo left; fi; "
             "rm -f " pidfile)))))

(defn wipe-data!
  "Deletes this node's WAL, so the next start is a genuinely fresh member rather
  than one carrying a log from a membership epoch it is no longer part of.

  Rejoining *with* the old log is a different scenario and not obviously a
  supported one; testing it by accident would manufacture findings this suite
  could not defend."
  [_test _node]
  (c/su
    (c/exec :rm :-rf wal-dir)
    (c/exec :mkdir :-p wal-dir)))

;; ---------------------------------------------------------------------------
;; Readiness
;; ---------------------------------------------------------------------------

(defn membership
  "The roster as seen by `node`: {:version long :members #{endpoint} :role str},
  or nil if the node cannot be reached.

  The roster is *changed* through the process lifecycle (--join-existing,
  --graceful-leave-on-shutdown), never through this endpoint — Kommander
  exposes JoinCluster/LeaveCluster on IRaft and nothing over HTTP."
  [node]
  (try+
    (let [r (kc/membership node)]
      (when (= 200 (:http-status r))
        {:version (:membershipVersion r)
         :members (set (map :endpoint (:members r)))
         :role    (:localRole r)}))
    (catch Object _ nil)))

(defn up?
  "Is this node answering API requests? Checks the HTTP status explicitly: the
  client returns a map even for error responses, so `some?` would always be
  true."
  [node]
  (try+
    (= 200 (:http-status (kc/health node)))
    (catch Object _ false)))

(defn voter?
  "Has `node` been committed to the roster as a Voter? Strictly stronger than
  `up?`: a node answers HTTP while it is still catching up as a Learner, and for
  several seconds at boot before the system partition even has a leader."
  [node]
  (= "Voter" (:role (membership node))))

(defn await-cluster!
  "Blocks until `node` is a committed Voter.

  Waiting for the HTTP port alone is not enough. The harness binds Kestrel
  *before* it joins — it has to, or every node would block on peers that are
  themselves blocked on binding — so a node answers /health within a second of
  starting and can spend far longer before the system partition has a leader.
  Starting the workload there produces a burst of routing failures that says
  nothing interesting."
  [node]
  (util/await-fn (fn [] (or (voter? node)
                            (throw (RuntimeException. "not a voter yet"))))
                 {:log-message (str "Waiting for Kommander harness on " node)
                  :timeout     180000
                  :interval    1000}))

(defn await-voter!
  "Blocks until `node` is a committed Voter. Returns true, or false on timeout —
  never throws, because a join that does not finish is a result the nemesis
  must record, not an error that aborts a test mid-history."
  ([node] (await-voter! node 90000))
  ([node timeout]
   (try+
     (util/await-fn (fn [] (or (voter? node)
                               (throw (RuntimeException. "not a voter yet"))))
                    {:log-message (str "Waiting for " node " to become a Voter")
                     :timeout     timeout
                     :interval    1000})
     true
     (catch Object _ false))))

(defn cluster-formed?
  "Does the whole cluster agree on a roster containing every node, with this
  node a Voter? Cheap precondition for a membership operation: removing a node
  from a cluster that has not finished forming tests nothing, and the departing
  node cannot commit its own removal because there is no leader to commit it."
  [test]
  (let [expected (count (:nodes test))]
    (every? (fn [node]
              (when-let [m (membership node)]
                (and (= "Voter" (:role m))
                     (= expected (count (:members m))))))
            (:nodes test))))

;; ---------------------------------------------------------------------------
;; Membership operations (driven by kommander.nemesis.membership)
;; ---------------------------------------------------------------------------

(defn leave!
  "Removes `node` from the cluster: stop it gracefully so its shutdown hook
  commits RemoveMember(self), then wipe its state so the next start is a fresh
  member. Runs in the caller's c/on-nodes context.

  Returns the graceful-stop outcome (:left, :killed or :not-running). Whether
  the roster actually shrank is checked by the caller against a *surviving*
  node — the departing node's own opinion is worthless, and an unverified
  :left is exactly how a no-op nemesis passes for a working one."
  [test node]
  (let [outcome (graceful-stop! test node)]
    (kill-stragglers!)
    (wipe-data! test node)
    outcome))

(defn join!
  "Adds `node` back: start it with --join-existing so it asks its seeds for
  admission as a Learner and is promoted once caught up.

  Waits for promotion to Voter, not merely for the HTTP port to answer:
  returning at the first 200 would report a join as complete while the cluster
  was still one voter short."
  [test node]
  (start! test node true)
  (if (await-voter! node)
    :joined
    :join-timeout))

;; ---------------------------------------------------------------------------

(defn db
  "A Kommander harness DB. `tarball` is a path on the *control node* to the
  self-contained publish produced by scripts/build-tarball.sh."
  [tarball]
  (reify
    db/DB
    (setup! [_ test node]
      (info node "installing Kommander harness from" tarball)
      (c/su
        (c/exec :mkdir :-p dir wal-dir)
        (c/upload tarball "/tmp/kommander.tar.gz")
        (c/exec :tar :xzf "/tmp/kommander.tar.gz" :-C dir)
        (c/exec :chmod :+x binary))
      (start! test node)
      (await-cluster! node))

    (teardown! [_ test node]
      (info node "tearing down Kommander harness")
      (stop! test node)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ _test _node]
      {logfile "kommander.log"})

    ;; Required by jepsen.nemesis.combined's :kill fault
    db/Process
    (start! [_ test node]
      (start! test node)
      :started)

    (kill! [_ _test _node]
      ;; Same self-kill hazard as stop!: the nemesis happily kills a node it
      ;; already killed, and that must not crash the test.
      (kill-stragglers!)
      :killed)

    ;; Required by jepsen.nemesis.combined's :pause fault
    db/Pause
    (pause! [_ _test _node]
      (c/su (cu/grepkill! :stop process-pattern))
      :paused)

    (resume! [_ _test _node]
      (c/su (cu/grepkill! :cont process-pattern))
      :resumed)))
