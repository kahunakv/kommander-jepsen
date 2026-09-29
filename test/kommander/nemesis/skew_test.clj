(ns kommander.nemesis.skew-test
  "The skew nemesis's generators. The nemesis itself talks to live harnesses;
  what can go quietly wrong without one is the shape of what it is asked to do."
  (:require [clojure.test :refer [deftest is testing]]
            [kommander.nemesis.skew :as skew]))

(def ^:private test-map {:nodes ["n1" "n2" "n3" "n4" "n5"]})

(deftest bumps-target-a-non-empty-subset-with-bounded-offsets
  (dotimes [_ 200]
    (let [{:keys [f value]} (skew/bump-gen test-map nil)]
      (is (= :skew-bump f))
      (is (seq value))
      (is (every? (set (:nodes test-map)) (keys value)))
      (is (every? #(<= 4 (Math/abs (long %)) (Math/pow 2 18)) (vals value))))))

(deftest strobes-carry-every-field-the-harness-needs
  (dotimes [_ 200]
    (let [{:keys [value]} (skew/strobe-gen test-map nil)]
      (is (seq value))
      (doseq [{:keys [delta period duration]} (vals value)]
        (is (pos? delta))
        (is (pos? period))
        (is (<= 0 duration 32000))))))

(deftest the-package-is-a-no-op-unless-asked-for
  (testing "a fault missing from --faults must not run, and one present must"
    (is (nil? (:nemesis (skew/package {:faults #{:partition}}))))
    (is (some? (:nemesis (skew/package {:faults #{:skew}}))))))
