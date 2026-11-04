(ns kommander.storage-test
  "Negative controls for the storage checker. Each test builds one violation by
  hand. Each test also checks a shape that looks like a violation and is legal."
  (:require [clojure.test :refer [deftest is testing]]
            [kommander.storage :as storage]))

(def ^:private clean-logs {"n1" {} "n2" {} "n3" {}})

(defn- response
  "A /debug/storage response. `partitions` is [[id state watermark log-index] …]."
  [partitions]
  {:status     "ok"
   :partitions (mapv (fn [[partition state watermark log-index]]
                       {:partition partition :state state
                        :watermark watermark :logIndex log-index})
                     partitions)})

(def ^:private healthy (response [[0 "Healthy" 10 10] [1 "Healthy" 20 20]]))

(defn- event
  "A :lost-tail-restart value: n3 acknowledged through 50 on partition 1."
  [partitions]
  {:victim     "n3"
   :watermarks {0 10, 1 50}
   :cut-to     100
   :storage    (response partitions)})

(deftest the-scan-finds-each-verdict-in-kommanders-own-words
  (testing "the lines come from RaftLoggerExtensions and RaftPartitionExecutor. If
            Kommander rewords them, the scan finds nothing, and this test does not
            see it. See the namespace docstring"
    (doseq [[signature line]
            {:lost-acknowledged-entries
             "crit: [n1:8082/1] Restore found acknowledged entries missing: this node had acknowledged ids through 9"
             :application-floor
             "crit: [n1:8082/1] The application's durable floor 3 is below the WAL compaction floor 9: the entries"
             :engine-refused-open
             "crit: RocksDB WAL at '/opt' refused to open: the storage engine reported corruption, and the node"
             :engine-fail-stop
             "crit: RocksDB WAL at '/opt' fail-stopped on a storage corruption: every write is refused"
             :partition-engine-failed
             "crit: [n1:8082/1/Follower] The storage engine fail-stopped: this partition steps down"
             :restore-failed
             "fail: [RaftPartitionExecutor/1] WAL restore (Phase 2) failed; partition will not process operations"}]
      (is (= {signature {:count 1 :first line}}
             (storage/scan-lines ["info: unrelated" line "info: unrelated"]))
          (str signature)))))

(deftest the-scan-counts-and-keeps-the-first-line
  (let [found (storage/scan-lines ["WAL restore (Phase 1) failed; one"
                                   "WAL restore (Phase 2) failed; two"])]
    (is (= 2 (get-in found [:restore-failed :count])))
    (is (= "WAL restore (Phase 1) failed; one" (get-in found [:restore-failed :first])))))

(deftest a-run-with-no-damage-and-no-verdict-is-valid
  (let [r (storage/check-storage {:logs clean-logs :final {"n1" healthy}})]
    (is (true? (:valid? r)))
    (is (= 3 (:logs-scanned r)))
    (is (= 2 (:storage-checked r)))))

(deftest restoring-and-not-hosted-are-not-verdicts
  (let [r (storage/check-storage
            {:logs clean-logs
             :final {"n1" (response [[0 "Restoring" -1 0] [1 "not-hosted" -1 0]])}})]
    (is (true? (:valid? r)))))

(deftest a-verdict-in-a-log-fails-a-run-with-no-damage
  (testing "the log is the only record of a quarantine that a repair cleared"
    (let [r (storage/check-storage
              {:logs  (assoc clean-logs "n2" {:lost-acknowledged-entries
                                              {:count 1 :first "the line"}})
               :final {"n2" healthy}})]
      (is (false? (:valid? r)))
      (is (= [{:node "n2" :signature :lost-acknowledged-entries
               :count 1 :first-line "the line"}]
             (:signatures r))))))

(deftest a-verdict-at-the-final-read-fails-a-run
  (testing "this half does not depend on message text"
    (let [r (storage/check-storage
              {:logs  clean-logs
               :final {"n1" (response [[1 "EngineFailed" 5 5]])}})]
      (is (false? (:valid? r)))
      (is (= "EngineFailed" (-> r :unhealthy first :state))))))

(deftest a-fail-stop-reason-fails-a-run
  (let [r (storage/check-storage
            {:logs  clean-logs
             :final {"n1" (assoc healthy :failStopReason "Corruption: bad block")}})]
    (is (false? (:valid? r)))
    (is (= [{:node "n1" :reason "Corruption: bad block"}] (:fail-stops r)))))

(deftest a-run-with-no-log-files-is-unknown
  (let [r (storage/check-storage {:logs {"n1" nil "n2" nil} :final {}})]
    (is (= :unknown (:valid? r)))
    (is (= :no-logs (:error r)))
    (is (= ["n1" "n2"] (:logs-missing r)))))

(deftest a-register-run-has-no-final-reading-and-still-passes-on-its-logs
  (let [r (storage/check-storage {:logs clean-logs :final {}})]
    (is (true? (:valid? r)))
    (is (zero? (:storage-checked r)))))

;; ---------------------------------------------------------------------------
;; Damage runs
;; ---------------------------------------------------------------------------

(deftest a-loss-that-reports-healthy-is-undetected
  (testing "the defect in Vorpal a516525b: the watermark falls with the rows"
    (let [r (storage/check-storage
              {:damage? true
               :logs    clean-logs
               :final   {"n3" healthy}
               :event   (event [[0 "Healthy" 10 10] [1 "Healthy" 30 30]])})]
      (is (false? (:valid? r)))
      (is (= [{:partition 1 :acknowledged 50 :present 30 :watermark 30 :state "Healthy"}]
             (-> r :damage :undetected))))))

(deftest a-detected-loss-passes-and-its-log-line-is-expected-on-the-victim
  (let [r (storage/check-storage
            {:damage? true
             :logs    (assoc clean-logs "n3" {:lost-acknowledged-entries
                                              {:count 1 :first "the line"}})
             :final   {"n3" healthy}
             :event   (event [[0 "Healthy" 10 10] [1 "LostAcknowledgedEntries" 50 30]])})]
    (is (true? (:valid? r)))
    (is (empty? (-> r :damage :undetected)))
    (is (= 1 (count (-> r :damage :lost))))))

(deftest a-verdict-on-any-other-node-still-fails-a-damage-run
  (testing "the damage explains the victim's line and nothing else"
    (let [r (storage/check-storage
              {:damage? true
               :logs    (assoc clean-logs "n1" {:lost-acknowledged-entries
                                                {:count 1 :first "the line"}})
               :final   {}
               :event   (event [[1 "LostAcknowledgedEntries" 50 30]])})]
      (is (false? (:valid? r))))))

(deftest the-same-log-line-with-no-damage-is-not-excused-by-a-stray-event
  (testing "only a damage run has a victim"
    (let [r (storage/check-storage
              {:logs  (assoc clean-logs "n3" {:lost-acknowledged-entries
                                              {:count 1 :first "the line"}})
               :final {}
               :event (event [[1 "LostAcknowledgedEntries" 50 30]])})]
      (is (false? (:valid? r))))))

(deftest a-damage-run-that-lost-nothing-is-unknown
  (testing "the truncation removed nothing, so the run tested nothing"
    (let [r (storage/check-storage
              {:damage? true
               :logs    clean-logs
               :final   {}
               :event   (event [[0 "Healthy" 10 10] [1 "Healthy" 50 50]])})]
      (is (= :unknown (:valid? r)))
      (is (= :no-loss-injected (:error r))))))

(deftest a-damage-run-with-no-reading-is-unknown
  (testing "the nemesis did not reach the restart, or the node did not answer"
    (is (= :no-restart-reading
           (:error (storage/check-storage
                     {:damage? true :logs clean-logs :final {}}))))
    (is (= :no-restart-reading
           (:error (storage/check-storage
                     {:damage? true :logs clean-logs :final {}
                      :event {:victim "n3" :watermarks {1 50}
                              :storage {:status :unreachable}}}))))))

(deftest a-loss-with-no-verdict-at-the-reading-is-unknown-not-undetected
  (let [r (storage/check-storage
            {:damage? true
             :logs    clean-logs
             :final   {}
             :event   (event [[1 "Restoring" 30 30]])})]
    (is (= :unknown (:valid? r)))
    (is (= :no-verdict-at-reading (:error r)))))

(deftest a-partition-with-no-watermark-before-the-damage-cannot-lose-anything
  (let [report (storage/damage-report
                 {:victim "n3" :watermarks {} :storage (response [[1 "Healthy" -1 7]])})]
    (is (empty? (:lost report)))))
