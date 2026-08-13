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
                       (let [m (get-in by-partition [p node])]
                         (if (seq m) (apply max (keys m)) 0)))

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
                             :hole? (< index (frontier node partition))}))

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
                      :diverged         diverged
                      :lost             lost
                      ;; Split out because the two demand different responses: a
                      ;; hole is a finding, a tail loss is first a prompt to
                      ;; raise --recovery-time and rerun.
                      :holes            (count (filter :hole? lost))
                      :tail-losses      (count (remove :hole? lost))
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
            nodes (->> ops
                       (filter #(and (= :ok (:type %)) (= :read-log (:f %))))
                       (map :value)
                       (reduce (fn [acc {:keys [node partitions]}]
                                 (assoc acc node partitions))
                               {}))]
        (check-logs acked nodes opts)))))

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

(defrecord LogClient [node partitions]
  client/Client
  (open! [this _test n]
    (assoc this :node n))

  (setup! [_ _test])

  (invoke! [_ _test op]
    (with-errors op
      (case (:f op)
        :append
        (let [{:keys [partition value]} (:value op)
              r (kc/log-append! node partition value {:timeout 5000})]
          (if (= "ok" (:status r))
            (assoc op :type :ok
                      :value {:partition partition :value value :index (:index r)})
            (assoc op :type (kc/response-class (:status r))
                      :error (:status r))))

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
    {:client          (LogClient. nil partitions)
     :checker         (checker opts)
     :generator       (appends partitions)
     ;; One final read per *thread*, after the cluster has healed. Threads are
     ;; bound to nodes round-robin, so this is the only shape that guarantees
     ;; every node is asked; `gen/once` would produce a single read from a
     ;; single node and the agreement check would have nothing to compare.
     ;; Threads sharing a node just re-read it, and the result map is keyed by
     ;; node, so the redundancy is free.
     :final-generator (gen/each-thread {:type :invoke, :f :read-log, :value nil})}))
