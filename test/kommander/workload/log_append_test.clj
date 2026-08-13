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
      (is (= [{:partition 1 :index 5 :value "v2" :node "n2" :found nil
               :harness-dropped false :hole? true}]
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
