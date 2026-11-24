(ns kommander.nemesis.lost-tail-test
  "The parts of the lost-tail scenario that can go wrong without a cluster. The
  arithmetic in `plan` is the scenario: with a different split, the victim is not
  in every commit, or the victim does not decide the election."
  (:require [clojure.test :refer [deftest is testing]]
            [kommander.core :as core]
            [kommander.nemesis.lost-tail :as lost-tail]))

(deftest the-plan-makes-the-victim-decide-both-majorities
  (doseq [nodes [["a" "b" "c"]
                 ["a" "b" "c" "d" "e"]
                 ["a" "b" "c" "d" "e" "f" "g"]]]
    (let [{:keys [victim old-side new-side]} (lost-tail/plan nodes)
          quorum (inc (quot (count nodes) 2))]
      (is (= quorum (inc (count old-side))) "a commit needs the victim")
      (is (= quorum (inc (count new-side))) "an election needs the victim")
      (is (= (set nodes) (into #{victim} (concat old-side new-side))))
      (is (empty? (filter (set old-side) new-side))))))

(deftest the-plan-refuses-a-cluster-with-no-such-split
  (doseq [nodes [[] ["a"] ["a" "b"] ["a" "b" "c" "d"]]]
    (is (thrown? IllegalArgumentException (lost-tail/plan nodes)))))

(deftest the-cut-removes-everything-above-the-recorded-size
  (is (= 100 (lost-tail/cut-size {:file "000004.log" :size 100}
                                 {:file "000004.log" :size 900}))))

(deftest the-cut-never-grows-a-file
  (testing "truncate -s to a larger size appends zeros, and zeros are not a loss"
    (is (= 100 (lost-tail/cut-size {:file "000004.log" :size 500}
                                   {:file "000004.log" :size 100})))))

(deftest a-rotated-log-is-cut-to-zero
  (is (zero? (lost-tail/cut-size {:file "000004.log" :size 100}
                                 {:file "000009.log" :size 900}))))

(deftest settled-needs-a-verdict-from-every-partition
  (is (lost-tail/settled? {:status "ok" :partitions [{:state "Healthy"}
                                                     {:state "LostAcknowledgedEntries"}]}))
  (is (not (lost-tail/settled? {:status "ok" :partitions [{:state "Healthy"}
                                                          {:state "Restoring"}]})))
  (is (not (lost-tail/settled? {:status "ok" :partitions [{:state "not-hosted"}]})))
  (is (not (lost-tail/settled? {:status "ok" :partitions []})))
  (is (not (lost-tail/settled? {:status :unreachable}))))

(deftest the-package-is-a-no-op-unless-asked-for
  (is (nil? (:nemesis (lost-tail/package {:faults #{:partition}}))))
  (is (some? (:nemesis (lost-tail/package {:faults #{:lost-tail}})))))

(deftest the-fault-runs-alone-on-log-append
  (is (nil? (core/check-fault-combination! :log-append #{:lost-tail})))
  (is (nil? (core/check-fault-combination! :register #{:partition :kill})))
  (is (thrown? IllegalArgumentException
               (core/check-fault-combination! :log-append #{:lost-tail :kill})))
  (is (thrown? IllegalArgumentException
               (core/check-fault-combination! :register #{:lost-tail}))))

(deftest all-does-not-damage-a-disk
  (is (not (contains? core/all-faults :lost-tail))))
