(ns kommander.workload.register
  "Linearizable-register workload over the harness's replicated key/value state
  machine.

  Each Jepsen key is one harness key holding a small integer. Operations are
  read / write / compare-and-set. Analysis is Knossos linearizability over
  jepsen's cas-register model, run per-key via jepsen.independent so the search
  space stays tractable.

  This is the workload that checks the core Raft claim: a committed entry is
  never lost, never reordered, and never observed out of order — through leader
  elections, partitions, crashes and catch-up.

  Two properties of the harness make the verdict trustworthy, and both are
  worth knowing before reading a red result:

  * **Reads are read-index reads.** The harness gates every read on
    `ConfirmLeadershipAsync`, a same-term quorum ack round. A minority-
    partitioned leader answers `leadership-unconfirmed` (a :fail) rather than
    serving stale state, so a stale read in the history is Kommander's, not the
    harness's.

  * **CAS is decided at apply time.** The comparison is evaluated on every
    replica at the entry's log position, not on the leader before proposing —
    see harness/StateMachine.cs for why the obvious alternative is unsound."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [kommander.client :as kc]
            [slingshot.slingshot :refer [try+]]))

(defn- parse-value
  "The harness stores opaque strings; we store decimal integers."
  [s]
  (when (and s (seq s)) (Long/parseLong s)))

(defmacro with-errors
  "Turns transport-level failures into Jepsen ops. A timeout or connection reset
  on a write is *indeterminate* (:info) — the proposal may still commit — so
  only operations we know never reached the server may be downgraded to :fail."
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

(defrecord RegisterClient [node]
  client/Client
  (open! [this _test n]
    (assoc this :node n))

  (setup! [_ _test])

  (invoke! [_ _test op]
    (let [[k v] (:value op)
          key   (str "jepsen/register/" k)
          opts  {:timeout 5000}]
      (with-errors op
        (case (:f op)
          :read
          (let [r (kc/kv-read node key opts)]
            (if (= "ok" (:status r))
              (assoc op :type :ok
                        :value (independent/tuple k (parse-value (:value r))))
              ;; A read that did not confirm tells us nothing about the register.
              ;; It must not be recorded as reading nil — that is a *claim*, and
              ;; a false one is indistinguishable from a lost write.
              (assoc op :type :fail :error (:status r))))

          :write
          (let [r (kc/kv-write! node key (str v) opts)]
            (assoc op :type (kc/response-class (:status r))
                      :error (when-not (= "ok" (:status r)) (:status r))))

          :cas
          (let [[old new] v
                r (kc/kv-cas! node key (str old) (str new) opts)]
            (assoc op :type (kc/response-class (:status r))
                      :error (when-not (= "ok" (:status r)) (:status r))))))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn- r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn- w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn- cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Options:
    :concurrency-per-key  processes hammering one register at once
    :ops-per-key          history length per key before rotating to a fresh one
    :linearizable-algorithm  :wgl (default) or :linear"
  [opts]
  {:client    (RegisterClient. nil)
   :checker   (independent/checker
                (checker/compose
                  ;; :wgl (Wing-Gong with Lowe's optimizations) uses far less
                  ;; memory than :linear on histories with many indeterminate
                  ;; ops, which is exactly what partition+kill runs produce.
                  {:linear   (checker/linearizable
                               {:model     (model/cas-register)
                                :algorithm (:linearizable-algorithm opts :wgl)})
                   :timeline (timeline/html)}))
   ;; Per-key concurrency is the dominant term in Knossos's search cost — it is
   ;; roughly exponential in the number of processes concurrently touching one
   ;; key. Turn this down before anything else when the analysis runs out of
   ;; memory; an unanalyzable history proves nothing.
   :generator (independent/concurrent-generator
                (:concurrency-per-key opts 3)
                (range)
                (fn [_k]
                  (->> (gen/mix [r w cas])
                       (gen/limit (:ops-per-key opts 100)))))})
