(ns kommander.workload.log-append
  "Per-partition log integrity: Raft's Log Matching Property, checked directly.

  The register workload asks whether the *state machine* behaves; this one asks
  whether the *log underneath it* does. Clients append opaque unique values to a
  named partition and, at the end of the run, every node is asked what it has
  applied for that partition. Four things must hold, and each maps to a claim
  Kommander makes:

  * **Agreement (Log Matching).** If two nodes have applied an entry at the
    same index, it must be the same entry. This is the property backfill and
    the snapshot handoff exist to preserve; a violation means a follower's log
    grew a divergent tail.

  * **Durability.** Every append the cluster *acknowledged* must be present, at
    the index it was acknowledged with, on every node that answers the final
    read. This is the one that catches a lost committed write after a kill, a
    membership change, or a catch-up round.

  * **Uniqueness.** No value may be applied twice. Kommander promises
    exactly-once delivery to the consumer
    (`RaftPartitionStateMachine.ApplyLogToConsumerAsync` withholds anything at
    or below the applied frontier), and the harness counts re-deliveries rather
    than silently swallowing them, so both a duplicated *entry* and a
    duplicated *delivery* are visible here.

  * **Monotonicity.** A node applies indices in increasing order.

  ## What is deliberately NOT checked

  **Gaps are legal.** The index space is shared with entries this workload never
  wrote — checkpoints, system-partition traffic, promotion barriers, other
  clients. A node's applied sequence is therefore expected to skip indices, and
  a `no gaps` check would fail every run while testing nothing Kommander
  claimed.

  **Order across partitions is not a property.** Each partition is an
  independent Raft group. There is no global order to violate, so nothing here
  compares indices between partitions.

  ## Why the final read waits for a condition, not a duration

  Every property above is read off the replicas *after* the faults stop, so all
  of them are hostage to when that read is taken. Take it while a node is still
  catching up and the checker sees the entries it has not got to yet as lost —
  a fabricated durability violation whose only cause is impatience.

  A fixed sleep can only be tuned against a machine. `await-convergence!`
  instead polls until every reporting node's applied frontier has reached the
  highest index the cluster acknowledged for that partition, which is the exact
  point at which a *tail* loss stops being possible: past it, a missing
  acknowledged entry is necessarily a hole. `--recovery-time` becomes the
  deadline on that wait rather than its duration, and a run that hits the
  deadline says so in `:convergence`, so a tail loss can be read as proven or
  unproven instead of guessed at.

  ## Why an empty run is :unknown, not clean

  All four properties are trivially satisfied by a history in which nothing was
  ever acknowledged, so a botched setup would read as a pass. Below
  `--min-appends` acknowledged appends the checker returns
  `{:valid? :unknown, :error :insufficient-data}` — the same trap, and the same
  answer, as an empty transaction graph."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [kommander.client :as kc]
            [slingshot.slingshot :refer [try+]]))

;; ---------------------------------------------------------------------------
;; Applied frontier — shared by the checker and the recovery wait
;; ---------------------------------------------------------------------------

(defn max-index
  "Highest index in `indices`, or 0 for nothing applied.

  This is a node's applied *frontier*, and it is defined once because two
  places depend on meaning the same thing by it: the checker, which uses it to
  tell a hole from a truncated tail, and the recovery wait, which uses it to
  decide when the final read may be taken. A wait measured on some other
  quantity would not be a wait for the condition the verdict turns on."
  [indices]
  (reduce max 0 indices))

(defn classify-frontier
  "Why a node/partition's applied frontier stopped where it did.

  `state` is one node's reported state for one partition. Returns one of:

    :caught-up      applied covers everything committed and contiguous here
    :undelivered    entries are present and committed above the applied frontier —
                    they could have been delivered and were not
    :uncommitted    the next entry is present but not committed, so withholding is
                    correct; the question is why it never commits
    :gap            the next entry is absent, so nothing above it is deliverable
                    until the leader re-ships it
    :unknown        the harness did not report a frontier (older build, or the scan
                    failed)

  These are three different subsystems — the apply path, the commit path, and
  replication — and the applied count alone cannot tell them apart. Six consecutive
  investigations picked the wrong one, so the verdict now says which.

  Compared against the node's *delivered* frontier, not against the highest index
  this workload recorded: the state machine also receives entries of other log
  types and advances over them, so using the workload's own maximum would
  manufacture undelivered entries out of ordinary foreign traffic."
  [state]
  (let [applied     (or (:applied-index state)
                        (max-index (map :index (:entries state))))
        committed   (:committed-index state)
        gap         (:first-gap-index state)
        uncommitted (:first-uncommitted-index state)]
    (cond
      (nil? committed)                :unknown

      ;; Ordered before :gap deliberately. Both can hold at once — entries
      ;; committed and undelivered *below* an absent id further up — and of the
      ;; two only this one is actionable now: those entries are on the node,
      ;; contiguous and committed, and could be delivered without anything
      ;; arriving from anywhere. The gap above is the next boundary, not the
      ;; current one, and reporting it instead hides the shortfall entirely.
      (> committed applied)           :undelivered

      (and gap (pos? gap))            :gap
      (and uncommitted
           (pos? uncommitted))        :uncommitted
      :else                           :caught-up)))

(defn lagging
  "Node/partitions that have not applied as far as the cluster acknowledged.

  `high-water` — {partition highest-acknowledged-index}
  `frontiers`  — {node {partition frontier}}, holding only the partitions each
                 node answered for

  Returns `[{:node :partition :frontier :needs} …]`, empty when every node that
  answered has caught up on every partition. A partition the node did not
  answer for counts as behind, with `:frontier nil`.

  Empty is the interesting state, because of what it rules out. If every
  acknowledged index sits at or below every node's frontier, then an
  acknowledged entry that is *missing* is missing from a position the node has
  already moved past — a hole, which no amount of further waiting would fill.
  So once this is empty the final read cannot produce a tail loss, and the
  ambiguity that makes a tail loss uninterpretable is gone rather than merely
  unlikely.

  Note this is a stronger condition than the frontiers simply agreeing with
  each other: five replicas can agree perfectly and still all sit below the
  high-water mark, which is exactly the run where every node reports the same
  missing tail. Agreement is not catching up."
  [high-water frontiers]
  (vec (for [[node ps]  frontiers
             [p needs]  high-water
             :let       [f (get ps p)]
             :when      (or (nil? f) (< f needs))]
         {:node node :partition p :frontier f :needs needs})))

(defn missing-nodes
  "Expected nodes that did not answer the poll at all.

  These hold the wait open too, which is worth being deliberate about. The
  checker cannot attribute a loss to a node that never answered, so a silent
  node is not a *risk* of a false violation — waiting on it buys no safety. It
  buys evidence: a node still replaying its WAL after a kill is one this run
  would otherwise never compare against, and a check across four replicas is
  weaker than one across five. The deadline bounds what that costs, and the
  worst case is the fixed sleep this replaced."
  [nodes frontiers]
  (vec (remove (set (keys frontiers)) nodes)))

;; ---------------------------------------------------------------------------
;; Checker — pure, so test/ can feed it violations directly
;; ---------------------------------------------------------------------------

(defn- index-map
  "A node's partition entries as {index value}. Later duplicates of an index are
  kept separately by `duplicate-indices`; this map is for agreement checks."
  [entries]
  (into {} (map (juxt :index :value)) entries))

(defn- duplicate-values
  "Values applied at more than one index on this node."
  [entries]
  (->> entries
       (group-by :value)
       (keep (fn [[v es]]
               (when (< 1 (count es))
                 {:value v :indices (mapv :index es)})))
       vec))

(defn- non-monotonic
  "Adjacent pairs where the index did not increase."
  [entries]
  (->> (partition 2 1 entries)
       (keep (fn [[a b]]
               (when-not (< (:index a) (:index b))
                 {:prev (:index a) :next (:index b)})))
       vec))

(defn- divergences
  "Indices where two nodes disagree about the entry, for one partition.
  `by-node` is {node {index value}}."
  [by-node]
  (->> (mapcat (fn [[node m]] (map (fn [[i v]] [i node v]) m)) by-node)
       (group-by first)
       (keep (fn [[index triples]]
               (let [values (set (map (fn [[_ _ v]] v) triples))]
                 (when (< 1 (count values))
                   {:index index
                    :values (into {} (map (fn [[_ n v]] [n v]) triples))}))))
       vec))

(defn check-logs
  "The whole verdict, as data.

  `acked`  — [{:partition p :index i :value v} …] appends the cluster confirmed
  `nodes`  — {node {partition {:entries [{:index :value} …] :redeliveries n}}}
  `opts`   — {:min-appends n}

  Returns a checker-shaped map. Kept free of Jepsen types on purpose: the
  negative controls in test/ construct these inputs directly, which is the only
  way to know the checker can actually fail."
  [acked nodes opts]
  (let [min-appends  (:min-appends opts 25)
        reporting    (vec (sort (keys nodes)))
        partitions   (sort (distinct (concat (map :partition acked)
                                             (mapcat (comp keys val) nodes))))

        ;; {partition {node {index value}}}
        by-partition (into {}
                           (for [p partitions]
                             [p (into {}
                                      (for [[node ps] nodes
                                            :when     (contains? ps p)]
                                        [node (index-map (:entries (get ps p)))]))]))

        diverged     (vec (mapcat (fn [[p by-node]]
                                    (map #(assoc % :partition p) (divergences by-node)))
                                  by-partition))

        ;; An acknowledged append must be present, at its index, on every node
        ;; that answered. "Answered" matters: a node that is down at the final
        ;; read cannot lose anything, and counting it would turn a slow restart
        ;; into a fabricated durability violation.
        ;; {[node partition] #{index …}} — indices the harness itself refused.
        undecodable  (into {}
                           (for [[node ps] nodes
                                 [p state] ps]
                             [[node p] (set (:undecodable state))]))

        ;; Highest index a node has applied for a partition, or 0. The boundary
        ;; between "never applied this" and "has not got here yet".
        frontier     (fn [node p]
                       (max-index (keys (get-in by-partition [p node]))))

        lost         (vec (for [{:keys [partition index value]} acked
                                node reporting
                                :let  [m (get-in by-partition [partition node])]
                                :when (and m (not= value (get m index)))]
                            {:partition partition
                             :index     index
                             :value     value
                             :node      node
                             :found     (get m index)
                             ;; True means the harness was handed this index and
                             ;; could not decode it, so the entry was dropped
                             ;; here rather than never delivered. Still a
                             ;; failure — but a different one, with a different
                             ;; culprit.
                             :harness-dropped
                             (contains? (get undecodable [node partition] #{})
                                        index)
                             ;; A *hole* is an absent index with applied entries
                             ;; beyond it — the node moved past this position
                             ;; without ever applying it, which no amount of
                             ;; waiting can fix. A loss past the node's frontier
                             ;; is a truncated tail, indistinguishable from a
                             ;; replica that simply had not caught up when the
                             ;; final read ran. Both are reported; only the first
                             ;; is unambiguous evidence on its own.
                             :hole? (< index (frontier node partition))
                             ;; True means Kommander *holds* this index on this
                             ;; node and never delivered it to the state machine:
                             ;; replication succeeded and the apply path did not.
                             ;; That is a different defect from the entry never
                             ;; arriving, in a different subsystem, and the two are
                             ;; indistinguishable from the applied entries alone —
                             ;; which is how four investigations in a row went
                             ;; looking for a replication fault that was not there.
                             ;; nil when the node did not report a log index (an
                             ;; older harness build).
                             :undelivered?
                             (when-let [li (get-in nodes [node partition :log-index])]
                               (<= index li))}))

        duplicated   (vec (for [[node ps] nodes
                                [p state] ps
                                dup       (duplicate-values (:entries state))]
                            (assoc dup :node node :partition p)))

        unordered    (vec (for [[node ps] nodes
                                [p state] ps
                                bad       (non-monotonic (:entries state))]
                            (assoc bad :node node :partition p)))

        redelivered  (vec (for [[node ps] nodes
                                [p state] ps
                                :when     (pos? (or (:redeliveries state) 0))]
                            {:node node :partition p :count (:redeliveries state)}))

        summary      {:acked-count      (count acked)
                      :nodes-reporting  reporting
                      :partitions       (vec partitions)
                      ;; Context, not a verdict — deliberately outside the
                      ;; emptiness check below. `{:converged? true}` says every
                      ;; reporting node had caught up to the acknowledged
                      ;; high-water mark before the final read, which is what
                      ;; makes a `:tail-losses` count below interpretable: with
                      ;; it, a tail loss is a real missing entry; without it,
                      ;; the run timed out waiting and a tail loss may be
                      ;; nothing but a slow replica. Absent for a run that did
                      ;; not wait at all.
                      :convergence      (:convergence opts)
                      :diverged         diverged
                      :lost             lost
                      ;; Split out because the two demand different responses: a
                      ;; hole is a finding, a tail loss is first a prompt to
                      ;; raise --recovery-time and rerun.
                      :holes            (count (filter :hole? lost))
                      :tail-losses      (count (remove :hole? lost))
                      ;; Cuts across holes and tail losses both: how many of the
                      ;; losses are entries the node physically holds. A run whose
                      ;; losses are mostly undelivered is not a replication
                      ;; problem, and chasing one is how the previous four rounds
                      ;; were spent.
                      :undelivered      (count (filter :undelivered? lost))
                      ;; Per node/partition, every frontier that matters and — the
                      ;; point of the exercise — *why* the applied one stopped. A
                      ;; bare applied-vs-log gap says only that something is wrong;
                      ;; :reason says which subsystem to look in. Raw numbers are
                      ;; carried alongside deliberately: the previous version of
                      ;; this field reported a derived boolean built on the max log
                      ;; id, and reading that boolean as though it meant "committed"
                      ;; produced a confident, wrong diagnosis.
                      :delivery         (vec (for [[node ps] nodes
                                                   [p state] ps
                                                   :let  [li (:log-index state)
                                                          ap (or (:applied-index state)
                                                                 (max-index (map :index (:entries state))))
                                                          reason (classify-frontier state)]
                                                   ;; Listed when the node is visibly behind, or when
                                                   ;; the frontier scan found something actionable.
                                                   ;; :unknown is deliberately not actionable — an
                                                   ;; older harness reports it for every partition,
                                                   ;; and listing them all would bury the real ones.
                                                   :when (and li (or (< ap li)
                                                                     (#{:undelivered :uncommitted :gap}
                                                                      reason)))]
                                               (cond-> {:node node :partition p
                                                        :applied ap :log li :behind (- li ap)
                                                        :reason reason}
                                                 (:committed-index state)
                                                 (assoc :committed (:committed-index state))

                                                 (and (:first-gap-index state)
                                                      (pos? (:first-gap-index state)))
                                                 (assoc :first-gap (:first-gap-index state)
                                                        ;; Carried with the gap so the judgement
                                                        ;; "absent above the floor, therefore
                                                        ;; missing" can be checked rather than
                                                        ;; trusted.
                                                        :floor (:checkpoint-floor state))

                                                 (and (:first-uncommitted-index state)
                                                      (pos? (:first-uncommitted-index state)))
                                                 (assoc :first-uncommitted (:first-uncommitted-index state)
                                                        :uncommitted-type (:first-uncommitted-type state)))))
                      ;; How many node/partitions landed in each category. The line
                      ;; to read first on a red run.
                      :frontiers        (frequencies
                                          (for [[_ ps] nodes
                                                [_ state] ps]
                                            (classify-frontier state)))
                      ;; Nodes whose log still says `Proposed` for entries they have
                      ;; already applied and serve. Deliberately *not* folded into
                      ;; :reason — a node here is usually `:caught-up` by its own
                      ;; frontier, and the damage lands on everyone else: the WAL read
                      ;; that feeds backfill filters uncommitted rows, so this node
                      ;; ships non-contiguous batches and every peer missing that range
                      ;; stays missing it. Read this list *first* when :gap is high and
                      ;; nothing else explains it.
                      :blocking-backfill
                      (vec (for [[node ps] nodes
                                 [p state] ps
                                 :let  [first-proposed (:first-proposed-below-applied state)]
                                 :when (and first-proposed (pos? first-proposed))]
                             {:node node :partition p
                              :first-proposed first-proposed
                              :count (:proposed-below-applied state)
                              :applied (or (:applied-index state)
                                           (max-index (map :index (:entries state))))}))
                      ;; Stale duplicates of resolved ids refused across the cluster.
                      ;; A total plus the per-node split, since one node counting
                      ;; orders of magnitude more than its peers is a different
                      ;; finding than all five counting a few. Non-zero is NOT a
                      ;; fault — duplicates arrive legitimately whenever a deposed
                      ;; leader is still broadcasting. -1 (partition not hosted) is
                      ;; filtered out, not summed.
                      ;;
                      ;; A 0 here does NOT mean the guard never fired. Kommander keeps
                      ;; the count in memory and `:kill` restarts the process, so it
                      ;; restarts too. Run 31811192785 measured it: the `partition`
                      ;; job (no restarts) reported 863 against 57 emitted log lines,
                      ;; while the kill jobs reported 7 and 5 against 19 and 16 lines
                      ;; — under the log, not over it. Under crash faults the node
                      ;; logs' "Skipped stale Proposed duplicate" lines are the record
                      ;; that survives; this field is a floor since the last restart.
                      ;; Key names the caveat: whoever reads results.edn sees only the
                      ;; number, and "since restart" is what stops a 0 being read as
                      ;; exoneration.
                      :stale-proposed-skipped-since-restart
                      (let [counts (for [[node ps] nodes
                                         [_p state] ps
                                         :let  [n (:stale-proposed-skipped state)]
                                         :when (and n (pos? n))]
                                     [node n])]
                        (if (seq counts)
                          {:total   (reduce + (map second counts))
                           :by-node (into (sorted-map)
                                          (for [[node xs] (group-by first counts)]
                                            [node (reduce + (map second xs))]))}
                          0))
                      :duplicated       duplicated
                      :unordered        unordered
                      :redelivered      redelivered
                      :undecodable      (vec (for [[[node p] idxs] undecodable
                                                   :when (seq idxs)]
                                               {:node node :partition p
                                                :indices (vec (sort idxs))}))}]

    (cond
      (empty? reporting)
      (assoc summary :valid? :unknown :error :no-final-reads)

      (< (count acked) min-appends)
      (assoc summary :valid? :unknown :error :insufficient-data)

      ;; `:undecodable` is in this list because it indicts the harness, not
      ;; Kommander — and a harness that silently drops entries would make every
      ;; other property on this list vacuous.
      (every? empty? [diverged lost duplicated unordered redelivered
                      (:undecodable summary)])
      (assoc summary :valid? true)

      :else
      (assoc summary :valid? false))))

(defn checker
  "Jepsen checker wrapping `check-logs`."
  [opts]
  (reify checker/Checker
    (check [_ _test history _checker-opts]
      (let [ops   (filter :type history)
            acked (->> ops
                       (filter #(and (= :ok (:type %)) (= :append (:f %))))
                       (map :value)
                       (map #(select-keys % [:partition :index :value]))
                       vec)
            ;; One per run, from the recovery phase. Reported, never used to
            ;; decide :valid? — see the note on :convergence in the summary.
            conv  (->> ops
                       (filter #(and (= :ok (:type %))
                                     (= :await-convergence (:f %))))
                       (map :value)
                       first)
            nodes (->> ops
                       (filter #(and (= :ok (:type %)) (= :read-log (:f %))))
                       (map :value)
                       (reduce (fn [acc {:keys [node partitions]}]
                                 (assoc acc node partitions))
                               {}))]
        (check-logs acked nodes (assoc opts :convergence conv))))))

;; ---------------------------------------------------------------------------
;; Recovery wait — poll until the replicas have caught up, not for a fixed time
;; ---------------------------------------------------------------------------

(defn with-progress
  "Annotates each lagging record with how far that node/partition moved since
  the first poll: `:first-frontier` and `:advanced` (the difference).

  This is the difference between a diagnosis and a shrug. A node sitting at
  frontier 52 when it needs 94 has two completely different stories — it is
  crawling and merely ran out of time, or it is wedged and would never have
  arrived — and the final frontier alone cannot tell them apart. `:advanced 0`
  across a three-minute quiescent window is not slowness, and says so without
  anyone having to open a 251 MB node log to find out."
  [lag first-frontiers]
  (mapv (fn [{:keys [node partition frontier] :as entry}]
          (let [first-f (get-in first-frontiers [node partition])]
            (assoc entry
                   :first-frontier first-f
                   :advanced (when (and first-f frontier) (- frontier first-f)))))
        lag))

(defn await-convergence!
  "Blocks until nothing is `lagging`, or the deadline passes.

  `poll!` is a thunk returning {node {partition frontier}}; injected rather
  than called directly so the loop can be tested without a cluster.

  Converged means every expected node answered *and* every one of them has
  caught up. Returns {:converged? :waited-ms :polls :lagging :missing}, which
  goes into the history and from there into the verdict. `:converged? false` is
  not itself a failure — it is the statement that the run ran out of patience,
  and therefore that any tail loss reported afterwards is unproven. On that
  path `:lagging` carries per-entry progress (see `with-progress`), because
  \"ran out of patience\" and \"would have waited forever\" warrant different
  responses and the distinction is free to record here.

  Timing is `nanoTime`, not `currentTimeMillis`: :clock is an available fault,
  and a deadline that a settimeofday can jump past would abandon the wait at
  the one moment it matters most."
  [poll! nodes high-water {:keys [recovery-time poll-interval]
                           :or   {recovery-time 90, poll-interval 2}}]
  (let [started  (System/nanoTime)
        deadline (+ started (* recovery-time 1000000000))
        elapsed  #(quot (- (System/nanoTime) started) 1000000)]
    (loop [polls 1, first-frontiers nil]
      (let [fr      (poll!)
            first-f (or first-frontiers fr)
            lag     (lagging high-water fr)
            missing (missing-nodes nodes fr)]
        (cond
          (and (empty? lag) (empty? missing))
          (do (info "replicas converged after" (elapsed) "ms," polls "polls")
              {:converged? true :waited-ms (elapsed) :polls polls
               :lagging [] :missing []})

          (<= deadline (System/nanoTime))
          (let [lag (with-progress lag first-f)]
            (info "recovery deadline reached;" (count lag)
                  "node/partitions still behind:" lag
                  "| nodes not answering:" missing)
            {:converged? false :waited-ms (elapsed) :polls polls
             :lagging lag :missing missing})

          :else
          ;; Coerced, not merely computed: --poll-interval parses with
          ;; read-string, so a fractional second arrives as a Double and
          ;; Thread/sleep has no double overload to receive it.
          (do (Thread/sleep (long (* poll-interval 1000)))
              (recur (inc polls) first-f)))))))

(defn poll-frontiers
  "Reads every node's applied frontier for every partition.

  Reuses `/log/entries` — the very endpoint the final read uses — so the wait
  is measured on the same data the checker will consume. A dedicated
  frontier-only endpoint would be cheaper, but it would also be a second code
  path that could disagree with the first, and a wait that agrees with the
  check matters more here than a few hundred KB per poll.

  A node that cannot be reached is omitted, which `missing-nodes` then treats
  as not-yet-converged. This is the rare place a bare `Exception` catch is
  right: every transport failure means the same thing to this loop — no
  frontier from that node this round — and a wait that propagated them would be
  defeated by the very faults it exists to recover from."
  [nodes partitions]
  (into {}
        (for [node nodes
              :let [ps (into {}
                             (for [p partitions
                                   :let [r (try (kc/log-entries node p {:timeout 10000})
                                                (catch Exception _ nil))]
                                   :when (= "ok" (:status r))]
                               [p (max-index (map :index (:entries r)))]))]
              :when (seq ps)]
          [node ps])))

;; ---------------------------------------------------------------------------
;; Client
;; ---------------------------------------------------------------------------

(defmacro with-errors
  [op & body]
  `(try+
     ~@body
     (catch java.net.ConnectException _#
       (assoc ~op :type :fail, :error :connection-refused))
     (catch java.net.SocketTimeoutException _#
       (assoc ~op :type :info, :error :timeout))
     (catch java.net.UnknownHostException _#
       (assoc ~op :type :fail, :error :unknown-host))
     (catch org.apache.http.NoHttpResponseException _#
       (assoc ~op :type :info, :error :no-http-response))
     (catch java.io.IOException e#
       (assoc ~op :type :info, :error [:io (.getMessage e#)]))))

;; `acked` is the highest index the cluster confirmed per partition, shared by
;; every client thread. The recovery wait needs it and only the clients see it:
;; an acknowledgement exists in the response to an append and nowhere else
;; until the history is checked, which is far too late to wait on.
(defrecord LogClient [node partitions acked opts]
  client/Client
  (open! [this _test n]
    (assoc this :node n))

  (setup! [_ _test])

  (invoke! [_ test op]
    (with-errors op
      (case (:f op)
        :append
        (let [{:keys [partition value]} (:value op)
              r (kc/log-append! node partition value {:timeout 5000})]
          (if (= "ok" (:status r))
            (do (swap! acked update partition (fnil max 0) (:index r))
                (assoc op :type :ok
                          :value {:partition partition :value value :index (:index r)}))
            (assoc op :type (kc/response-class (:status r))
                      :error (:status r))))

        ;; Not a workload operation — the recovery wait, run as an op so that
        ;; it happens on a worker thread, is ordered before the final reads by
        ;; gen/phases, and lands in the history where the verdict and anyone
        ;; reading the store can see how long convergence actually took.
        :await-convergence
        (assoc op :type :ok
                  :value (await-convergence! #(poll-frontiers (:nodes test) partitions)
                                             (:nodes test)
                                             @acked
                                             opts))

        :read-log
        ;; Read every partition from *this* client's node. Each client is bound
        ;; to one node, so a final read per client is a read per node — which is
        ;; the whole point: the check compares replicas against each other.
        (let [results (into {}
                            (for [p partitions
                                  :let [r (kc/log-entries node p {:timeout 30000})]
                                  :when (= "ok" (:status r))]
                              [p {:entries      (mapv #(select-keys % [:index :value])
                                                      (:entries r))
                                  :redeliveries (:redeliveries r)
                                  ;; The highest log id Kommander holds for this
                                  ;; partition, delivered or not. Lets the checker
                                  ;; say whether a missing entry is absent from the
                                  ;; node or merely undelivered on it — two findings
                                  ;; with nothing in common but their symptom.
                                  :log-index    (:logIndex r)
                                  ;; Where the node's log stops being contiguously
                                  ;; committed, and why. See `frontier` in the
                                  ;; summary: these three turn "applied is behind"
                                  ;; into a statement about which subsystem broke.
                                  :committed-index         (:committedIndex r)
                                  :first-gap-index         (:firstGapIndex r)
                                  :first-uncommitted-index (:firstUncommittedIndex r)
                                  :first-uncommitted-type  (:firstUncommittedType r)
                                  :checkpoint-floor        (:checkpointFloor r)
                                  ;; Rows this node still has as Proposed *below* its
                                  ;; own applied frontier — it serves them as committed
                                  ;; and its log disagrees. Invisible to every other
                                  ;; measure here, and the reason a whole partition can
                                  ;; starve while the responsible node looks healthy.
                                  :first-proposed-below-applied (:firstProposedBelowApplied r)
                                  :proposed-below-applied       (:proposedBelowApplied r)
                                  ;; Stale duplicates of already-resolved ids this node
                                  ;; refused to write. Expected to be non-zero under
                                  ;; faults; recorded because the guard that drops them
                                  ;; is silent per-occurrence and is what prevents a
                                  ;; resolved row regressing to Proposed and then being
                                  ;; truncated away. Against the two rows above, zero
                                  ;; here and a storm here point at opposite causes.
                                  :stale-proposed-skipped       (:staleProposedSkipped r)
                                  ;; The node's *delivered* frontier, which is not the
                                  ;; same as the highest index this workload recorded:
                                  ;; the state machine also receives entries of other
                                  ;; log types, and its frontier advances over those
                                  ;; too. Comparing a committed frontier against the
                                  ;; workload's own max index would understate delivery
                                  ;; and invent undelivered entries.
                                  :applied-index           (:appliedIndex r)
                                  ;; Indices the harness was handed but could
                                  ;; not decode. Without these, "missing on one
                                  ;; node" is ambiguous between a Kommander
                                  ;; delivery failure and a harness drop.
                                  :undecodable  (vec (:undecodable r))}]))]
          (if (seq results)
            (assoc op :type :ok :value {:node node :partitions results})
            (assoc op :type :fail :error :no-entries))))))

  (teardown! [_ _test])

  (close! [_ _test]))

;; ---------------------------------------------------------------------------
;; Generator
;; ---------------------------------------------------------------------------

(defn- appends
  "An endless stream of :append ops carrying globally unique values.

  Uniqueness is what makes duplicate detection meaningful, and it comes from a
  counter rather than from the value's content: two clients appending the same
  string would be indistinguishable from one entry applied twice."
  [partitions]
  (let [counter (atom 0)]
    (fn [_test _ctx]
      {:type  :invoke
       :f     :append
       :value {:partition (rand-nth partitions)
               :value     (str "v" (swap! counter inc))}})))

(defn workload
  "Options:
    :partitions   number of Raft partitions the cluster was started with
    :min-appends  acknowledged appends below which the verdict is :unknown"
  [opts]
  (let [;; Application partitions are 1..N inclusive — partition 0 is reserved
        ;; for replicated system configuration and rejects client proposals.
        partitions (vec (range 1 (inc (:partitions opts 4))))]
    (info "log-append over partitions" partitions)
    {:client          (LogClient. nil partitions (atom {}) opts)
     :checker         (checker opts)
     :generator       (appends partitions)
     ;; Runs after the nemesis has healed and before the final read. Only this
     ;; workload has one: the register workload compares client-observed
     ;; operations rather than replicas, so replica catch-up cannot affect its
     ;; verdict and waiting for it would be dead time.
     :await-generator (gen/once {:type :invoke, :f :await-convergence, :value nil})
     ;; One final read per *thread*, after the cluster has healed. Threads are
     ;; bound to nodes round-robin, so this is the only shape that guarantees
     ;; every node is asked; `gen/once` would produce a single read from a
     ;; single node and the agreement check would have nothing to compare.
     ;; Threads sharing a node just re-read it, and the result map is keyed by
     ;; node, so the redundancy is free.
     :final-generator (gen/each-thread {:type :invoke, :f :read-log, :value nil})}))
