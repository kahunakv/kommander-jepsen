(ns kommander.client
  "Thin Clojure wrapper over the Jepsen harness API.

  Kommander is a *library*, not a server: `Kommander.Server` exposes only the
  inter-node Raft transport (`/v1/raft/append-logs`, `/request-vote`, …) and a
  demo replication loop. There is no application state machine to check for
  linearizability, so this suite ships its own — `harness/`, a small ASP.NET
  host that embeds `IRaft`, applies committed entries to a replicated key/value
  map, and exposes the register and log-append operations these workloads
  drive. See DESIGN.md for what the harness does and does not decide.

  Everything is plain JSON over plain HTTP; `response-class` below is where a
  server answer becomes a Jepsen verdict."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]))

(def http-port
  "The harness client API. Deliberately a different port from the Raft
  transport (kommander.db/raft-port) so a network fault can break replication
  without also killing the client connection — otherwise every partition would
  produce indeterminate operations for a reason that has nothing to do with
  consensus."
  8081)

(defn base-url [node]
  (str "http://" node ":" http-port))

;; ---------------------------------------------------------------------------
;; Classification — the single most important function in this suite
;; ---------------------------------------------------------------------------

(def definite-failures
  "Statuses that mean the operation was rejected *before* anything could be
  appended to any log, so it definitely did not take effect.

  Each one is here because a specific rejection happens ahead of the append:

    not-leader / no-leader / node-is-not-leader
        Routing. The harness never called ReplicateLogs, or Kommander refused
        to propose because this node does not lead the partition.
    leadership-unconfirmed
        A read whose read-index round did not confirm. Nothing was written.
    no-partition-map / invalid-partition
        The request never became a proposal.
    cas-mismatch
        The strongest kind of definite failure: the entry committed and the
        comparison did not hold. The write did not happen, and we know it from
        the applied state rather than by inference.
    proposal-queue-full / active-proposal / restore-in-progress
        Admission control, checked before the entry is created.
    insufficient-voters / stale-membership / concurrent-membership-change
        Membership-layer rejections, likewise pre-append.

  Anything not listed here and not `ok` falls through to :info. That asymmetry
  is deliberate: a wrong :fail manufactures phantom violations, while a wrong
  :info only costs search time."
  #{"not-leader" "no-leader" "node-is-not-leader"
    "leadership-unconfirmed"
    "no-partition-map" "invalid-partition"
    "cas-mismatch"
    "proposal-queue-full" "active-proposal" "restore-in-progress"
    "insufficient-voters" "stale-membership" "concurrent-membership-change"
    "partition-moved"})

(defn response-class
  "Classifies a harness status string for a *write*:

    :ok    — the operation definitely took effect
    :fail  — the operation definitely did NOT take effect
    :info  — indeterminate; it may or may not apply later

  The indeterminate set is everything else, and it is worth naming the ones
  that actually show up under a nemesis:

    proposal-timeout      quorum did not answer in time; the entry is appended
                          and may commit later
    replication-failed    the round failed, but not before the entry existed
    errored / pending     status unknown to the proposer
    operation-cancelled   the await was cut short; the queued work may still
                          apply
    apply-timeout         a CAS that *committed* but whose effect this node had
                          not applied yet, so whether the comparison held is
                          genuinely unknown
    forward-failed        the harness proxied to the leader and lost the
                          response — the classic lost-acknowledgement case
    leader-in-old-term / logs-from-another-leader / log-mismatch /
    snapshot-required / proposal-not-found
                          replication-path statuses that can be observed after
                          an entry reached some log

  Before trusting a red result, re-read this mapping. See DESIGN.md."
  [status]
  (cond
    (nil? status)                :info
    (= "ok" status)              :ok
    (definite-failures status)   :fail
    :else                        :info))

;; ---------------------------------------------------------------------------
;; Transport
;; ---------------------------------------------------------------------------

(defn- normalize
  "clj-http's :as :json only parses when the response *is* JSON. A 500 with an
  ASP.NET text/plain body, or an empty body, yields a String — assoc'ing onto
  that throws ClassCastException mid-history. Non-map bodies become a map with
  no :status, which `response-class` treats as indeterminate."
  [resp]
  (let [body (:body resp)]
    (if (map? body)
      (assoc body :http-status (:status resp))
      {:status nil :http-status (:status resp) :body body})))

(defn post!
  "POSTs `body` as JSON to `path` on `node`. Returns the parsed body with
  keyword keys. Never throws on a non-2xx status — callers decide. Network-level
  failures still throw and are funnelled through each workload's `with-errors`."
  ([node path body] (post! node path body {}))
  ([node path body opts]
   (let [timeout (:timeout opts 5000)]
     (normalize
       (http/post (str (base-url node) path)
                  {:body               (json/generate-string body)
                   :content-type       :json
                   :accept             :json
                   :socket-timeout     timeout
                   :connection-timeout timeout
                   :throw-exceptions   false
                   :as                 :json})))))

(defn get!
  ([node path] (get! node path {}))
  ([node path opts]
   (let [timeout (:timeout opts 5000)]
     (normalize
       (http/get (str (base-url node) path)
                 {:accept             :json
                  :socket-timeout     timeout
                  :connection-timeout timeout
                  :throw-exceptions   false
                  :as                 :json})))))

;; ---------------------------------------------------------------------------
;; Key/value register
;; ---------------------------------------------------------------------------

(defn kv-read
  "Reads `key`. The harness serves this only after a Raft read-index round
  confirms leadership, so a successful read is linearizable; an unconfirmed one
  answers `leadership-unconfirmed` rather than stale state.

  Returns {:status str :value str-or-nil}."
  [node key opts]
  (post! node "/kv/read" {:key key} opts))

(defn kv-write!
  "Unconditional write of `value` (a string, or nil to delete)."
  [node key value opts]
  (post! node "/kv/write" {:key key :value value} opts))

(defn kv-cas!
  "Compare-and-set on the value. Note this is decided when the entry is
  *applied*, not when it is proposed, so `cas-mismatch` is a statement about
  the state at the entry's log position — see harness/StateMachine.cs."
  [node key expected value opts]
  (post! node "/kv/cas" {:key key :expected expected :value value} opts))

;; ---------------------------------------------------------------------------
;; Raw partition log
;; ---------------------------------------------------------------------------

(defn log-append!
  "Appends an opaque value to `partition`'s Raft log. Returns {:status :index}
  where :index is the log index Kommander assigned."
  [node partition value opts]
  (post! node "/log/append" {:partition partition :value value} opts))

(defn log-entries
  "Everything `node` has *applied* for `partition`, in apply order, plus the
  node's redelivery counter. Read from each node separately on purpose: the
  point is to compare replicas, so a routing layer that quietly answered from
  the leader would defeat the check."
  [node partition opts]
  (get! node (str "/log/entries/" partition) opts))

;; ---------------------------------------------------------------------------
;; Cluster
;; ---------------------------------------------------------------------------

(defn membership
  "Reads /cluster/membership — the committed roster as this node sees it."
  ([node] (membership node {:timeout 2000}))
  ([node opts] (get! node "/cluster/membership" opts)))

(defn health
  ([node] (health node {:timeout 2000}))
  ([node opts] (get! node "/health" opts)))
