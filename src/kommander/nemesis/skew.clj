(ns kommander.nemesis.skew
  "Clock-skew nemesis: moves each node's hybrid-logical-clock time on its own.

  ## Why not jepsen.nemesis.time

  Jepsen's clock nemesis calls `settimeofday`, and every container on a host
  shares one kernel clock. Bumping n1 bumps all five, so under Docker the
  `:clock` fault is a cluster-wide jump and never skew *between* nodes — the
  thing an HLC exists to tolerate. It also needs CAP_SYS_TIME and moves the
  host's clock with it.

  This nemesis instead asks each harness to add an offset to the physical time
  its HLC reads (`/debug/clock`, see harness/SkewedClock.cs). The skew is real
  and per node, and it needs no privileges, so it runs on Docker Desktop and CI.
  Only the HLC moves: Kommander's elapsed-time gates run on monotonic ticks,
  which `settimeofday` would not have moved either.

  ## Operations

  Modelled on jepsen.nemesis.time, with offsets that are absolute rather than
  cumulative so a run cannot drift a node arbitrarily far:

    :skew-bump    {node offset-ms}  set a constant offset on some nodes
    :skew-strobe  {node {:delta :period :duration}}  flip a node's clock by
                  :delta ms and back every :period ms for :duration ms
    :skew-reset   nodes, or nil for all — back to true time

  ## What to read in the history

  Every operation reports the clock of *every* node afterwards, not only the
  ones it targeted, as {node {:offset ms :hlc-lead ms}}. :hlc-lead is how far
  the node's HLC runs ahead of true time. A node with :offset 0 and a large
  :hlc-lead was pulled forward by a peer — the HLC merging a remote timestamp,
  which is how one fast clock reaches the whole cluster. After a forward bump
  the lead outlives :skew-reset, because an HLC never moves back; that is
  expected, and it is the state the counter component then has to carry.

  A node answering :unreachable was down or paused. A node answering 404 was
  started without --enable-clock-skew, which means the fault did nothing there
  — see `kommander.db/skew-faults?`.

  ## Kill and skew together

  The offset lives in the harness process, so a node killed while skewed
  restarts on true time: a wall-clock correction across a restart. Kommander
  restores a persisted HLC floor on start for exactly this case, so
  `--faults skew,kill` is the combination that exercises it."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]]
            [kommander.client :as kc]))

(def ^:private max-exponent
  "Offsets are ±2^(2..2+max-exponent) ms, the same spread as
  jepsen.nemesis.time: from 4 ms up to about 262 s, weighted toward the small
  end so both sub-heartbeat and multi-election skews occur."
  16)

(defn- rand-magnitude-ms []
  (long (Math/pow 2 (+ 2 (rand max-exponent)))))

(defn- rand-subset
  "A non-empty random subset of `nodes`."
  [nodes]
  (let [shuffled (shuffle (vec nodes))]
    (take (inc (rand-int (count shuffled))) shuffled)))

(defn bump-gen
  "Offsets for a random subset of nodes. The generator picks them, not the
  nemesis, so a replayed generator reproduces the same skews."
  [test _ctx]
  {:type  :info
   :f     :skew-bump
   :value (into (sorted-map)
                (for [node (rand-subset (:nodes test))]
                  [node (* (rand-nth [-1 1]) (rand-magnitude-ms))]))})

(defn strobe-gen [test _ctx]
  {:type  :info
   :f     :skew-strobe
   :value (into (sorted-map)
                (for [node (rand-subset (:nodes test))]
                  [node {:delta    (rand-magnitude-ms)
                         :period   (long (Math/pow 2 (rand 10)))
                         :duration (long (* 1000 (rand 32)))}]))})

(defn reset-gen [test _ctx]
  {:type :info, :f :skew-reset, :value (vec (rand-subset (:nodes test)))})

(defn- node-result
  "Folds a harness clock-state response (or a failure) into the few fields the
  history needs."
  [resp]
  (cond
    (= "ok" (:status resp)) {:offset   (:offsetMs resp)
                             :strobing (:strobing resp)
                             :hlc-lead (:hlcLeadMs resp)}
    (:status resp)          {:error (:status resp)}
    :else                   {:error [:http (:http-status resp)]}))

(defn- call
  "Runs `f` against `node`, turning a connection failure into a result rather
  than an exception: a killed or paused node is an expected state here."
  [node f]
  (try
    (node-result (f node))
    (catch Exception e
      {:error [:unreachable (.getMessage e)]})))

(defn- on-nodes
  "Calls `f` on every node in parallel and returns {node result}."
  [nodes f]
  (->> nodes
       (mapv (fn [node] [node (future (call node f))]))
       (into (sorted-map) (map (fn [[node fut]] [node @fut])))))

(defn- request
  "Translates one :value entry into the harness request body."
  [f setting]
  (case f
    :skew-bump   {:offsetMs setting}
    :skew-strobe {:offsetMs         0
                  :strobeDeltaMs    (:delta setting)
                  :strobePeriodMs   (:period setting)
                  :strobeDurationMs (:duration setting)}
    :skew-reset  {:offsetMs 0}))

(defn nemesis
  []
  (reify n/Nemesis
    (setup! [this test]
      ;; Start from true time even if a previous run left a harness skewed.
      (on-nodes (:nodes test) #(kc/set-clock! % {:offsetMs 0}))
      this)

    (invoke! [_ test op]
      (let [f        (:f op)
            settings (case f
                       :skew-reset (zipmap (or (:value op) (:nodes test))
                                           (repeat nil))
                       (:value op))
            applied  (on-nodes (keys settings)
                               #(kc/set-clock! % (request f (get settings %))))
            ;; Every node, not only the targets: the untargeted ones are where
            ;; propagation shows up.
            observed (on-nodes (:nodes test) kc/clock-state)]
        (info "skew" f (:value op) "->" observed)
        (when-let [failed (seq (filter (comp :error val) applied))]
          (warn "skew" f "did not apply on" (mapv key failed)))
        (assoc op :value {:requested (:value op)
                          :applied   applied
                          :clocks    observed})))

    (teardown! [_ test]
      (on-nodes (:nodes test) #(kc/set-clock! % {:offsetMs 0})))

    n/Reflection
    (fs [_] #{:skew-bump :skew-strobe :skew-reset})))

(defn package
  "A nemesis package shaped like the ones jepsen.nemesis.combined returns.
  Returns the no-op package unless :skew is in :faults."
  [opts]
  (if-not (some #{:skew} (:faults opts))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    {:generator       (->> (gen/mix [bump-gen strobe-gen reset-gen])
                           (gen/stagger (:interval opts 15)))
     ;; Every node back to true time before the final reads. The HLC lead a
     ;; forward bump left behind stays, and that is fine: the final reads check
     ;; order, not closeness to real time.
     :final-generator {:type :info, :f :skew-reset, :value nil}
     :nemesis         (nemesis)
     :perf            #{{:name  "skew"
                         :start #{:skew-bump :skew-strobe}
                         :stop  #{:skew-reset}
                         :color "#E9C3A0"}}}))
