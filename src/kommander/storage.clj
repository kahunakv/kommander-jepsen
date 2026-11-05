(ns kommander.storage
  "Checks Kommander's storage verdicts (`IRaft.GetPartitionStorageHealth`).

  A run with no disk damage must produce no verdict. A false quarantine takes a
  voter out of elections for no reason, and the other checkers cannot see that.
  A run with disk damage must produce a verdict on the damaged node.

  ## Two sources, because each one misses a case

  **Node logs.** Each verdict writes one line at Critical or Error level. The
  logs survive a restart, and a quarantine that a restart or a repair cleared is
  still on record there. The logs depend on Kommander's message text. If the text
  changes, the scan finds nothing and says nothing.

  **`/debug/storage` at the final read.** This does not depend on message text,
  but it shows only the state at the end of the run. Only the log-append workload
  takes this reading. :storage-checked counts the partitions it read, and 0 means
  this half of the check did nothing.

  ## Damage runs

  With :damage? true the nemesis records what the victim acknowledged before the
  damage and what it holds after the restart. A partition that holds less and
  reports Healthy is :undetected. A run where the victim lost nothing is :unknown,
  because the test injected no fault."
  (:require [clojure.java.io :as io]
            [jepsen [checker :as checker]
                    [store :as store]]))

(def signatures
  "One pattern per verdict, from Kommander's `RaftLoggerExtensions` and
  `RaftPartitionExecutor`, and from `SolariumWAL`, whose engine lines use their own
  words."
  {:lost-acknowledged-entries #"Restore found acknowledged entries missing"
   :application-floor         #"The application's durable floor \S+ is below"
   :engine-refused-open       #"refused to open: the storage engine reported corruption|refuses to open on storage damage"
   :engine-fail-stop          #"fail-stopped on a storage corruption|is fail-stopped on storage damage"
   :partition-engine-failed   #"The storage engine fail-stopped"
   :restore-failed            #"WAL restore \(Phase \d\) failed"
   :engine-lost-synced-writes #"lost synced writes: the sync ledger"
   :partition-lost-synced-writes #"Restore found the storage engine lost synced writes"
   :hard-state-restored       #"restored the hard state of partition \d+ from the hard-state ledger"
   ;; Warnings, not verdicts. Each one says that a ledger was reset, and the check
   ;; for lost synced writes then starts over. No crash produces either one.
   :sync-ledger-foreign       #"belongs to another (engine|database)"
   :sync-ledger-unreadable    #"holds no readable record|no slot of the sync ledger .* passes its checksum"})

(def expected-on-victim
  "Correct reactions to the damage, on the damaged node only."
  #{:lost-acknowledged-entries :engine-refused-open
    :engine-lost-synced-writes :partition-lost-synced-writes
    :hard-state-restored})

(def ^:private no-verdict-yet
  "States that say nothing yet about the storage."
  #{"Restoring" "not-hosted"})

(defn scan-lines
  "Returns {signature {:count n :first line}} for the lines that match."
  [lines]
  (reduce (fn [acc line]
            (reduce-kv (fn [a signature pattern]
                         (if (re-find pattern line)
                           (-> a
                               (update-in [signature :count] (fnil inc 0))
                               (update-in [signature :first] #(or % line)))
                           a))
                       acc
                       signatures))
          {}
          lines))

(defn- explained?
  "Does the damage in this run explain this match?"
  [victim {:keys [node signature]}]
  (and (= node victim) (expected-on-victim signature)))

(defn- signature-matches
  [logs]
  (vec (for [[node found]                 logs
             [signature {:keys [count first]}] found]
         {:node node :signature signature :count count :first-line first})))

(defn unhealthy-partitions
  "Every partition that holds a storage verdict. `final` is {node /debug/storage
  response}."
  [final]
  (vec (for [[node response] final
             p               (:partitions response)
             :when (not (or (= "Healthy" (:state p))
                            (no-verdict-yet (:state p))))]
         (assoc (select-keys p [:partition :state :reason]) :node node))))

(defn damage-report
  "Compares what the victim acknowledged before the damage with what it holds
  after the restart.

  `event` is the value of the :lost-tail-restart operation. :lost holds the
  partitions with a shorter log. :undetected is the subset that reports Healthy.
  :unsettled is the subset with no verdict at the time of the reading."
  [{:keys [victim watermarks storage cut-to position-at-kill]}]
  (if-not (= "ok" (:status storage))
    {:victim victim :error :no-restart-reading}
    (let [lost (vec (for [p     (:partitions storage)
                          :let  [before (get watermarks (:partition p) -1)]
                          :when (< (:logIndex p) before)]
                      {:partition    (:partition p)
                       :acknowledged before
                       :present      (:logIndex p)
                       :watermark    (:watermark p)
                       :state        (:state p)}))
          report {:victim           victim
                  :position-at-kill position-at-kill
                  :cut-to           cut-to
                  :lost             lost
                  :undetected       (filterv #(= "Healthy" (:state %)) lost)
                  :unsettled        (filterv #(no-verdict-yet (:state %)) lost)}]
      (cond
        (empty? lost)              (assoc report :error :no-loss-injected)
        (and (empty? (:undetected report))
             (seq (:unsettled report))) (assoc report :error :no-verdict-at-reading)
        :else                      report))))

(defn check-storage
  "The whole verdict, as data, so that test/ can feed it violations.

  `logs`    {node {signature {:count :first}}}, with nil for a missing log file
  `final`   {node /debug/storage response} from the final read
  `event`   the value of the :lost-tail-restart operation, or nil
  `damage?` true when this run damages a disk"
  [{:keys [logs final event damage?]}]
  (let [victim      (when damage? (:victim event))
        matches     (signature-matches (remove (comp nil? val) logs))
        found       (vec (remove (partial explained? victim) matches))
        unhealthy   (unhealthy-partitions final)
        fail-stops  (vec (for [[node response] final
                               :when (:failStopReason response)]
                           {:node node :reason (:failStopReason response)}))
        report      (when damage?
                      (if event
                        (damage-report event)
                        {:error :no-restart-reading}))
        result      {:logs-scanned    (count (remove (comp nil? val) logs))
                     :logs-missing    (vec (sort (keys (filter (comp nil? val) logs))))
                     :signatures      found
                     ;; The victim's reaction to the damage. Evidence, not a verdict.
                     :expected-signatures (filterv (partial explained? victim) matches)
                     ;; 0 means the final-read half of this check did nothing.
                     :storage-checked (reduce + (map (comp count :partitions) (vals final)))
                     :unhealthy       unhealthy
                     :fail-stops      fail-stops}
        result      (cond-> result report (assoc :damage report))]
    (cond
      (or (seq found) (seq unhealthy) (seq fail-stops) (seq (:undetected report)))
      (assoc result :valid? false)

      (zero? (:logs-scanned result))
      (assoc result :valid? :unknown :error :no-logs)

      (:error report)
      (assoc result :valid? :unknown :error (:error report))

      :else
      (assoc result :valid? true))))

(defn- scan-log
  [^java.io.File file]
  (when (.exists file)
    (with-open [reader (io/reader file)]
      (scan-lines (line-seq reader)))))

(defn- final-readings
  [history]
  (into {}
        (for [op    history
              :when (and (= :ok (:type op)) (= :read-log (:f op)))
              :let  [{:keys [node storage]} (:value op)]
              :when (= "ok" (:status storage))]
          [node storage])))

(defn- restart-event
  "The completion of :lost-tail-restart. Its invocation has no :value map."
  [history]
  (->> history
       (filter #(and (= :lost-tail-restart (:f %)) (map? (:value %))))
       (map :value)
       first))

(defn checker
  "Reads the node logs from the store, so it needs no live cluster. Jepsen copies
  the logs there before analysis."
  [{:keys [damage?]}]
  (reify checker/Checker
    (check [_ test history _opts]
      (check-storage
        {:logs    (into {}
                        (for [node (:nodes test)]
                          [node (scan-log (store/path test (name node) "kommander.log"))]))
         :final   (final-readings history)
         :event   (restart-event history)
         :damage? damage?}))))
