(ns kommander.core-test
  "Tests for the verdict machinery in `kommander.core`.

  Currently one subject: the stats checker, which is the only place this suite
  reinterprets a Jepsen verdict rather than producing its own. Reinterpreting a
  verdict is exactly the kind of change that can quietly turn a real failure
  green, so the tests below pin both directions — the downgrade it is supposed
  to make, and the failures it must leave alone."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [kommander.core :as core]))

(defn- history
  "A history of client ops with the given [f type] pairs. `jepsen.checker/stats`
  ignores :invoke entries and keys everything off :f and :type, so this is
  enough to drive it — but it must be a real `jepsen.history` and not a plain
  vector, because the checker folds over it with tesser."
  [pairs]
  (h/history (map-indexed (fn [i [f type]]
                            {:index i :time i :process 0 :type type :f f :value nil})
                          pairs)))

(defn- check [h]
  (checker/check (core/stats-checker) {} h {}))

(deftest a-run-where-every-op-type-succeeded-is-valid
  (let [r (check (history [[:read :ok] [:write :ok] [:cas :ok]]))]
    (is (true? (:valid? r)))
    (is (nil? (:error r)))))

(deftest an-op-type-that-never-succeeded-is-unknown-not-false
  (testing "zero successful CAS means the run did not test CAS — absence of
            evidence, which must not be reported in the same word as a
            violation"
    (let [r (check (history [[:read :ok]  [:read :ok]
                             [:write :ok]
                             [:cas :fail] [:cas :fail] [:cas :info]]))]
      (is (= :unknown (:valid? r)))
      (is (= :no-successful-ops (:error r)))
      (is (= [:cas] (:no-successful-ops r))))))

(deftest every-starved-op-type-is-named
  (testing "the actionable part: which operations never once got through"
    (let [r (check (history [[:read :ok] [:write :fail] [:cas :fail]]))]
      (is (= :unknown (:valid? r)))
      (is (= [:cas :write] (:no-successful-ops r))))))

(deftest the-underlying-counts-survive-the-downgrade
  (testing "the reinterpretation must not cost the numbers that explain it"
    (let [r (check (history [[:read :ok] [:cas :fail] [:cas :info]]))]
      (is (= 3 (:count r)))
      (is (= 1 (:ok-count r)))
      (is (= 1 (:fail-count r)))
      (is (= 1 (:info-count r)))
      (is (= 0 (get-in r [:by-f :cas :ok-count]))))))

(deftest an-unknown-verdict-still-fails-the-run
  (testing "this downgrade relabels a red run, it does not make one green:
            jepsen exits 1 on false and 2 on :unknown, and :unknown dominates
            true when checkers are composed. A test that let this silently
            become a pass would defeat the point of the suite"
    (let [r (check (history [[:read :ok] [:cas :fail]]))]
      (is (not (true? (:valid? r))))
      (is (= :unknown (checker/merge-valid [true (:valid? r)]))))))
