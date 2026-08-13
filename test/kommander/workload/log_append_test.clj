(ns kommander.workload.log-append-test
  "Negative controls for the log-append checker.

  A checker that never fails is worse than no checker: it converts every run
  into a green tick that means nothing. These tests construct each violation by
  hand and assert the checker rejects it — and, just as importantly, assert it
  *accepts* the shapes that look like violations but are legal (gaps in the
  index space, a node that was down for the final read)."
  (:require [clojure.test :refer [deftest is testing]]
            [kommander.workload.log-append :as la]))

(defn- entries
  "[[index value] …] -> the shape the harness returns."
  [pairs]
  (mapv (fn [[i v]] {:index i :value v}) pairs))

(defn- node-state
  ([pairs] (node-state pairs 0))
  ([pairs redeliveries]
   {1 {:entries (entries pairs) :redeliveries redeliveries}}))

(def ^:private acked
  [{:partition 1 :index 3 :value "v1"}
   {:partition 1 :index 5 :value "v2"}
   {:partition 1 :index 9 :value "v3"}])

(def ^:private clean-node
  (node-state [[3 "v1"] [5 "v2"] [9 "v3"]]))

(def ^:private opts {:min-appends 3})

(deftest accepts-a-clean-run
  (let [r (la/check-logs acked {"n1" clean-node "n2" clean-node "n3" clean-node} opts)]
    (is (true? (:valid? r)) (pr-str r))))

(deftest gaps-in-the-index-space-are-legal
  (testing "the index space is shared with checkpoints, system traffic and
            other clients, so an applied sequence is expected to skip indices"
    (let [sparse (node-state [[3 "v1"] [5 "v2"] [9 "v3"]])
          r      (la/check-logs acked {"n1" sparse "n2" sparse} opts)]
      (is (true? (:valid? r)) (pr-str r)))))

(deftest detects-divergence
  (testing "two nodes with different entries at the same index"
    (let [diverged (node-state [[3 "v1"] [5 "OTHER"] [9 "v3"]])
          r        (la/check-logs acked {"n1" clean-node "n2" diverged} opts)]
      (is (false? (:valid? r)))
      (is (= 1 (count (:diverged r))))
      (is (= 5 (:index (first (:diverged r))))))))

(deftest detects-a-lost-acknowledged-append
  (testing "an entry the cluster acknowledged is missing from a replica"
    (let [lossy (node-state [[3 "v1"] [9 "v3"]])
          r     (la/check-logs acked {"n1" clean-node "n2" lossy} opts)]
      (is (false? (:valid? r)))
      ;; :undelivered? is nil rather than false — this node reported no log index,
      ;; so whether it holds the entry is unknown, not answered in the negative.
      (is (= [{:partition 1 :index 5 :value "v2" :node "n2" :found nil
               :harness-dropped false :hole? true :undelivered? nil}]
             (:lost r))))))

(deftest distinguishes-a-hole-from-a-truncated-tail
  (testing "an absent index with entries applied *beyond* it cannot be a replica
            still catching up — no amount of waiting fills it in — whereas a
            missing tail is exactly what a slow node looks like. Conflating them
            turns a tuning problem into a bug report and vice versa"
    (let [hole   (node-state [[3 "v1"] [9 "v3"]])          ; 5 absent, 9 applied
          tail   (node-state [[3 "v1"] [5 "v2"]])          ; stops before 9
          r-hole (la/check-logs acked {"n1" clean-node "n2" hole} opts)
          r-tail (la/check-logs acked {"n1" clean-node "n2" tail} opts)]
      (is (false? (:valid? r-hole)))
      (is (= 1 (:holes r-hole)))
      (is (= 0 (:tail-losses r-hole)))
      (is (true? (:hole? (first (:lost r-hole)))))

      (is (false? (:valid? r-tail)))
      (is (= 0 (:holes r-tail)))
      (is (= 1 (:tail-losses r-tail)))
      (is (false? (:hole? (first (:lost r-tail))))))))

(deftest a-node-with-nothing-applied-is-all-tail-loss
  (testing "an empty partition has frontier 0, so every loss is past it — this
            must not be misreported as N unambiguous holes"
    (let [empty-p {1 {:entries [] :redeliveries 0}}
          r       (la/check-logs acked {"n1" clean-node "n2" empty-p} opts)]
      (is (false? (:valid? r)))
      (is (= 0 (:holes r)))
      (is (= 3 (:tail-losses r))))))

(deftest detects-an-append-that-moved-index
  (testing "acknowledged at 5, applied at 6 — the acknowledgement was a lie
            about where the entry landed"
    (let [moved (node-state [[3 "v1"] [6 "v2"] [9 "v3"]])
          r     (la/check-logs acked {"n1" moved} opts)]
      (is (false? (:valid? r)))
      (is (seq (:lost r))))))

(deftest detects-a-duplicated-value
  (testing "one value applied at two indices"
    (let [dup (node-state [[3 "v1"] [5 "v2"] [7 "v2"] [9 "v3"]])
          r   (la/check-logs acked {"n1" dup} opts)]
      (is (false? (:valid? r)))
      (is (= "v2" (:value (first (:duplicated r))))))))

(deftest detects-non-monotonic-application
  (let [backwards (node-state [[3 "v1"] [9 "v3"] [5 "v2"]])
        r         (la/check-logs acked {"n1" backwards} opts)]
    (is (false? (:valid? r)))
    (is (seq (:unordered r)))))

(deftest detects-redeliveries
  (testing "Kommander promises exactly-once delivery to the consumer; the
            harness counts violations rather than swallowing them"
    (let [r (la/check-logs acked {"n1" (node-state [[3 "v1"] [5 "v2"] [9 "v3"]] 2)} opts)]
      (is (false? (:valid? r)))
      (is (= 2 (:count (first (:redelivered r))))))))

(deftest a-node-that-did-not-answer-cannot-lose-anything
  (testing "a node still restarting at the final read is absent, not empty —
            counting it would fabricate a durability violation out of a slow
            boot"
    (let [r (la/check-logs acked {"n1" clean-node "n2" clean-node} opts)]
      (is (true? (:valid? r)))
      (is (= ["n1" "n2"] (:nodes-reporting r))))))

(deftest a-node-reporting-an-empty-partition-does-lose
  (testing "answering the read with nothing applied is a claim, and a false one"
    (let [r (la/check-logs acked {"n1" clean-node "n2" (node-state [])} opts)]
      (is (false? (:valid? r)))
      (is (= 3 (count (:lost r)))))))

(deftest too-few-appends-is-unknown-not-clean
  (testing "every property here is trivially satisfied by an empty history, so
            a botched setup must not read as a pass"
    (let [r (la/check-logs [(first acked)]
                           {"n1" (node-state [[3 "v1"]])}
                           {:min-appends 25})]
      (is (= :unknown (:valid? r)))
      (is (= :insufficient-data (:error r))))))

(deftest no-final-reads-is-unknown
  (let [r (la/check-logs acked {} opts)]
    (is (= :unknown (:valid? r)))
    (is (= :no-final-reads (:error r)))))

(deftest attributes-a-loss-the-harness-caused
  (testing "an entry the harness was handed but could not decode is still a
            failure, but it is the harness's failure — the distinction is the
            difference between a Kommander bug report and a patch to this repo"
    (let [dropped {1 {:entries      (entries [[3 "v1"] [9 "v3"]])
                      :redeliveries 0
                      :undecodable  [5]}}
          r       (la/check-logs acked {"n1" clean-node "n2" dropped} opts)]
      (is (false? (:valid? r)))
      (is (= 1 (count (:lost r))))
      (is (true? (:harness-dropped (first (:lost r)))))
      (is (= [{:node "n2" :partition 1 :indices [5]}] (:undecodable r))))))

(deftest an-undecodable-entry-alone-fails-the-run
  (testing "a harness that silently drops deliveries makes every other property
            vacuous, so this must fail even when nothing was acknowledged lost"
    (let [odd {1 {:entries      (entries [[3 "v1"] [5 "v2"] [9 "v3"]])
                  :redeliveries 0
                  :undecodable  [7]}}
          r   (la/check-logs acked {"n1" clean-node "n2" odd} opts)]
      (is (false? (:valid? r)))
      (is (empty? (:lost r)))
      (is (= [7] (:indices (first (:undecodable r))))))))

(deftest a-missing-undecodable-key-is-not-a-violation
  (testing "older harness builds omit the field entirely; absent must read as
            'nothing dropped', not as a nil that trips the emptiness check"
    (let [r (la/check-logs acked {"n1" clean-node "n2" clean-node} opts)]
      (is (true? (:valid? r)))
      (is (empty? (:undecodable r))))))

;; ---------------------------------------------------------------------------
;; Absent on the node vs held but never delivered
;; ---------------------------------------------------------------------------

(deftest an-entry-the-node-holds-but-never-delivered-is-marked-undelivered
  (testing "the node's log reaches index 9, so v3 is *on* it and was simply never
            handed to the state machine — replication worked and the apply path
            did not. Reported distinctly because the two live in different
            subsystems and look identical in the applied entries alone"
    (let [held {1 {:entries (entries [[3 "v1"] [5 "v2"]]) :redeliveries 0 :log-index 9}}
          r    (la/check-logs acked {"n1" clean-node "n2" held} opts)]
      (is (false? (:valid? r)))
      (is (= 1 (:undelivered r)))
      (is (true? (:undelivered? (first (:lost r)))))
      (is (= [{:node "n2" :partition 1 :applied 5 :log 9 :behind 4}] (:delivery r))))))

(deftest an-entry-the-node-never-received-is-not-marked-undelivered
  (testing "the log stops below the missing index, so the entry genuinely never
            arrived — this one really is a replication question"
    (let [absent {1 {:entries (entries [[3 "v1"] [5 "v2"]]) :redeliveries 0 :log-index 5}}
          r      (la/check-logs acked {"n1" clean-node "n2" absent} opts)]
      (is (false? (:valid? r)))
      (is (= 0 (:undelivered r)))
      (is (false? (:undelivered? (first (:lost r)))))
      (is (empty? (:delivery r))))))

(deftest a-hole-can-also-be-undelivered
  (testing "the classification cuts across holes and tail losses rather than
            replacing them: an index the node moved past can still be one it holds"
    (let [hole {1 {:entries (entries [[3 "v1"] [9 "v3"]]) :redeliveries 0 :log-index 20}}
          r    (la/check-logs acked {"n1" clean-node "n2" hole} opts)]
      (is (= 1 (:holes r)))
      (is (= 1 (:undelivered r))))))

(deftest a-harness-without-the-log-index-reports-nil-not-false
  (testing "older harness builds omit the field; guessing 'not undelivered' would
            silently mis-file every loss in the run it matters for"
    (let [old {1 {:entries (entries [[3 "v1"] [5 "v2"]]) :redeliveries 0}}
          r   (la/check-logs acked {"n1" clean-node "n2" old} opts)]
      (is (nil? (:undelivered? (first (:lost r)))))
      (is (= 0 (:undelivered r)))
      (is (empty? (:delivery r))))))

;; ---------------------------------------------------------------------------
;; The recovery wait
;; ---------------------------------------------------------------------------

(deftest lagging-is-empty-once-every-node-reaches-the-high-water-mark
  (is (empty? (la/lagging {1 9} {"n1" {1 9} "n2" {1 12}}))))

(deftest lagging-names-the-node-and-how-far-behind-it-is
  (is (= [{:node "n2" :partition 1 :frontier 4 :needs 9}]
         (la/lagging {1 9} {"n1" {1 9} "n2" {1 4}}))))

(deftest agreeing-frontiers-are-not-enough
  (testing "the condition is catching up, not agreeing: five replicas can agree
            perfectly and still all sit below the acknowledged high-water mark,
            which is precisely the run where every node reports the same
            missing tail. A wait that stopped at agreement would stop there"
    (is (seq (la/lagging {1 40} {"n1" {1 30} "n2" {1 30} "n3" {1 30}})))))

(deftest a-partition-a-node-did-not-answer-for-holds-the-wait-open
  (testing "an unanswered partition is not a caught-up one; treating absence as
            satisfied would let the read proceed against a node that might yet
            have answered"
    (is (= [{:node "n1" :partition 2 :frontier nil :needs 5}]
           (la/lagging {1 9, 2 5} {"n1" {1 9}})))))

(deftest a-node-with-nothing-applied-does-hold-the-wait-open
  (testing "frontier 0 is a re-joined Learner mid-backfill — the case the wait
            exists for, and the one a frontier of 'absent' must not be confused
            with"
    (is (= [{:node "n1" :partition 1 :frontier 0 :needs 9}]
           (la/lagging {1 9} {"n1" {1 0}})))))

(deftest nothing-acknowledged-means-nothing-to-wait-for
  (testing "a run the kill nemesis flattened acknowledges nothing and is headed
            for :insufficient-data — there is no point sleeping through it"
    (is (empty? (la/lagging {} {"n1" {1 0} "n2" {1 0}})))))

(deftest a-silent-node-is-waited-for
  (testing "it cannot produce a false violation — the checker skips it — but a
            node still replaying its WAL after a kill is one more replica to
            compare against, and four is a weaker check than five"
    (is (= ["n3"] (la/missing-nodes ["n1" "n2" "n3"] {"n1" {1 9} "n2" {1 9}})))
    (is (empty? (la/missing-nodes ["n1" "n2"] {"n1" {1 9} "n2" {1 9}})))))

(deftest await-returns-as-soon-as-the-cluster-catches-up
  (testing "the point of polling: a healthy cluster must not pay the deadline"
    (let [polls  (atom 0)
          poll!  (fn [] (swap! polls inc)
                        (if (< @polls 3) {"n1" {1 2}} {"n1" {1 9}}))
          r      (la/await-convergence! poll! ["n1"] {1 9}
                                        {:recovery-time 60 :poll-interval 0})]
      (is (true? (:converged? r)))
      (is (= 3 (:polls r)))
      (is (empty? (:lagging r)))
      (is (empty? (:missing r))))))

(deftest await-waits-for-a-node-that-is-not-answering-yet
  (testing "caught up is not enough while a replica is still coming back"
    (let [polls (atom 0)
          poll! (fn [] (swap! polls inc)
                       (if (< @polls 4) {"n1" {1 9}} {"n1" {1 9} "n2" {1 9}}))
          r     (la/await-convergence! poll! ["n1" "n2"] {1 9}
                                       {:recovery-time 60 :poll-interval 0})]
      (is (true? (:converged? r)))
      (is (= 4 (:polls r))))))

(deftest await-gives-up-at-the-deadline-and-says-so
  (testing "a wait that timed out must be distinguishable from one that
            succeeded, or the tail losses that follow cannot be read"
    (let [r (la/await-convergence! (constantly {"n1" {1 2}}) ["n1" "n2"] {1 9}
                                   {:recovery-time 0 :poll-interval 0})]
      (is (false? (:converged? r)))
      (is (= [{:node "n1" :partition 1 :frontier 2 :needs 9
               :first-frontier 2 :advanced 0}]
             (:lagging r)))
      (is (= ["n2"] (:missing r))))))

(deftest a-timed-out-wait-reports-whether-the-replica-was-moving
  (testing "'ran out of time' and 'was never going to arrive' need different
            responses, and the final frontier alone cannot tell them apart"
    (let [stuck   (la/with-progress [{:node "n1" :partition 1 :frontier 52 :needs 94}]
                                    {"n1" {1 52}})
          crawling (la/with-progress [{:node "n1" :partition 1 :frontier 90 :needs 94}]
                                     {"n1" {1 52}})]
      (is (= 0 (:advanced (first stuck))) "wedged: no progress at all")
      (is (= 38 (:advanced (first crawling))) "slow, but converging")
      (is (= 52 (:first-frontier (first crawling)))))))

(deftest progress-is-nil-rather-than-wrong-when-there-is-no-baseline
  (testing "a node that only appeared after the first poll has no starting
            frontier; inventing one would report a wedged replica as moving"
    (let [r (la/with-progress [{:node "n2" :partition 1 :frontier 7 :needs 9}]
                              {"n1" {1 3}})]
      (is (nil? (:first-frontier (first r))))
      (is (nil? (:advanced (first r)))))))

(deftest the-verdict-records-whether-the-wait-converged
  (testing "reported, but never decisive: :convergence must not turn a clean
            run red, and must not turn a violation green"
    (let [conv  {:converged? false :waited-ms 90000 :polls 45 :lagging []}
          clean (la/check-logs acked {"n1" clean-node}
                               (assoc opts :convergence conv))
          dirty (la/check-logs acked {"n1" (node-state [[3 "v1"] [9 "v3"]])}
                               (assoc opts :convergence
                                      {:converged? true :waited-ms 12 :polls 2}))]
      (is (true? (:valid? clean)))
      (is (= conv (:convergence clean)))
      (is (false? (:valid? dirty)))
      (is (true? (:converged? (:convergence dirty)))))))

(deftest violations-are-found-across-partitions
  (testing "each partition is an independent Raft group; a divergence in one
            must not be masked by another being clean"
    (let [good {1 {:entries (entries [[3 "v1"]]) :redeliveries 0}
                2 {:entries (entries [[4 "w1"]]) :redeliveries 0}}
          bad  {1 {:entries (entries [[3 "v1"]]) :redeliveries 0}
                2 {:entries (entries [[4 "ZZZ"]]) :redeliveries 0}}
          r    (la/check-logs [{:partition 1 :index 3 :value "v1"}
                               {:partition 2 :index 4 :value "w1"}]
                              {"n1" good "n2" bad}
                              {:min-appends 2})]
      (is (false? (:valid? r)))
      (is (= 2 (:partition (first (:diverged r))))))))
