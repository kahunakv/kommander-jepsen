(ns kommander.core
  "Entry point. `lein run test --help` for options."
  (:gen-class)
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [kommander.db :as kdb]
            [kommander.nemesis.membership :as membership]
            [kommander.workload.log-append :as log-append]
            [kommander.workload.register :as register]))

(def workloads
  {:register   register/workload
   :log-append log-append/workload})

(def all-faults
  "Clock faults are NOT in the default set on purpose: settimeofday inside a
  container moves the shared kernel clock, which on Docker Desktop means the
  whole VM. Enable :clock only on a disposable Linux host — but do enable it
  there, because Kommander stamps proposal tickets with a hybrid logical clock
  and that is where the interesting bugs live."
  #{:partition :kill :pause :membership})

(defn parse-faults [s]
  (if (= s "all")
    all-faults
    (set (map keyword (str/split s #",")))))

(defn kommander-test
  "Builds a test map from CLI options."
  [opts]
  (let [workload-fn (workloads (:workload opts))
        workload    (workload-fn opts)
        faults      (:faults opts all-faults)
        db          (kdb/db (:tarball opts))
        nemesis-opts {:db        db
                      :nodes     (:nodes opts)
                      :faults    faults
                      :partition {:targets [:one :majority :majorities-ring]}
                      :kill      {:targets [:one :majority :all]}
                      :pause     {:targets [:one :majority]}
                      :interval  (:nemesis-interval opts 15)
                      :membership-interval (:membership-interval opts 30)}
        ;; :membership is ours, not jepsen.nemesis.combined's — it would ignore
        ;; the fault silently and the test would run with no membership churn
        ;; at all, which is exactly the kind of quiet no-op that reads as a
        ;; clean pass. Composed in explicitly instead.
        nemesis     (nc/compose-packages
                      (conj (nc/nemesis-packages nemesis-opts)
                            (membership/package nemesis-opts)))]
    (merge tests/noop-test
           opts
           {:name       (str "kommander-" (name (:workload opts))
                             "-" (str/join "," (map name (sort faults))))
            :os         debian/os
            :db         db
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf       (checker/perf {:nemeses (:perf nemesis)})
                           :stats      (checker/stats)
                           :exceptions (checker/unhandled-exceptions)
                           :workload   (:checker workload)})
            :generator  (gen/phases
                          (->> (:generator workload)
                               (gen/stagger (/ (:rate opts 15)))
                               (gen/nemesis (:generator nemesis))
                               (gen/time-limit (:time-limit opts)))
                          (gen/log "Healing cluster")
                          (gen/nemesis (:final-generator nemesis))
                          ;; Long enough for a re-joined Learner to be
                          ;; backfilled and promoted. The final read compares
                          ;; replicas; taking it while one is still catching up
                          ;; reports catch-up latency as a durability
                          ;; violation.
                          ;;
                          ;; 60 rather than 30 on measured evidence: a
                          ;; partition-fault run on a 4-CPU Docker Desktop VM
                          ;; left a node 53 entries behind on one partition, and
                          ;; 30 s of healed network was not enough to close the
                          ;; gap — the same run passed cleanly at 90 s. Too
                          ;; short here does not hide bugs, it manufactures
                          ;; them.
                          (gen/log "Waiting for recovery")
                          (gen/sleep (:recovery-time opts 60))
                          (gen/clients (:final-generator workload)))})))

(def cli-opts
  [["-w" "--workload NAME" "Workload to run"
    :default :register
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--tarball PATH" "Path (on the control node) to the harness tarball"
    :default "target/kommander-harness.tar.gz"]

   [nil "--faults FAULTS" "Comma-separated nemesis faults, or 'all'"
    :default all-faults
    :parse-fn parse-faults]

   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations"
    :default 15
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--membership-interval SECONDS" "Seconds between membership operations.
                                        Longer than --nemesis-interval on
                                        purpose: a leave waits for the departing
                                        node to commit its own removal, and a
                                        join waits for a Learner to catch up and
                                        be promoted."
    :default 30
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--recovery-time SECONDS" "Seconds to wait after healing, before the
                                  final read. Must cover a Learner's backfill
                                  and promotion or the log-append checker sees
                                  a lagging replica as a lost write."
    :default 60
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   ["-r" "--rate HZ" "Approximate request rate per client"
    :default 15
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--partitions COUNT" "Raft partitions (independent Raft groups). The
                             harness routes register keys by hash across all of
                             them; the log-append workload addresses them
                             directly."
    :default 4
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--transport NAME" "Inter-node transport: grpc (default, the production
                           path) or rest"
    :default :grpc
    :parse-fn keyword
    :validate [#{:grpc :rest} "must be grpc or rest"]]

   [nil "--disable-wal-sync-writes" "Run without WAL fsync (expect data loss on kill)"
    :default false]

   [nil "--linearizable-algorithm NAME" "Knossos search: wgl (lower memory,
                                        default) or linear."
    :default :wgl
    :parse-fn keyword
    :validate [#{:wgl :linear} "must be wgl or linear"]]

   [nil "--concurrency-per-key COUNT" "Processes hammering one register at once.
                                      Drives Knossos's search cost hardest.
                                      MUST divide --concurrency evenly, or
                                      jepsen.independent asserts at start-up."
    :default 3
    :parse-fn read-string]

   [nil "--ops-per-key COUNT" "Operations per register before rotating keys.
                              Higher values make Knossos's search exponentially
                              more expensive; 100-200 is the practical ceiling."
    :default 100
    :parse-fn read-string]

   [nil "--min-appends COUNT" "Acknowledged appends below which the log-append
                              verdict is :unknown rather than clean. An empty
                              history satisfies every property it checks."
    :default 25
    :parse-fn read-string
    :validate [pos? "must be positive"]]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  kommander-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
