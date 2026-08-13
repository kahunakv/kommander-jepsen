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

(defn stats-checker
  "`jepsen.checker/stats`, with its one failure mode reported as :unknown.

  `stats` fails a run when any `:f` has zero `:ok` operations — `:valid? (pos?
  oks)` in its fold is the only thing it ever tests. That is a real signal, but
  it is *absence of evidence*, not evidence of a violation, and the two deserve
  different words. The register workload's CAS succeeds only when its expected
  value happens to match the register, so a run whose cluster was unavailable
  for most of its duration can legitimately land on zero: run 31709747704 failed
  exactly this way, `:cas` at 0 of 432 with 77 of 1308 operations succeeding
  overall. A run that tested nothing was reported as a run that found something.

  :unknown is the honest verdict, and the one this suite already reaches for in
  the same situation: `--min-appends` turns a log-append run with too few
  acknowledged appends into :unknown rather than a pass, on identical reasoning
  — a property no operation exercised is neither satisfied nor violated.

  Note this does not make such a run *green*, and is not meant to. Jepsen exits
  1 on false and 2 on :unknown, so CI stays red either way; what changes is that
  the red now says which kind it is."
  []
  (let [inner (checker/stats)]
    (reify checker/Checker
      (check [_ test history checker-opts]
        (let [result (checker/check inner test history checker-opts)]
          (if (false? (:valid? result))
            (assoc result
                   :valid? :unknown
                   :error  :no-successful-ops
                   ;; Which :f never once succeeded — the actionable part, and
                   ;; not otherwise obvious in a :by-f map of a dozen entries.
                   :no-successful-ops
                   (vec (sort (keep (fn [[f s]] (when (false? (:valid? s)) f))
                                    (:by-f result)))))
            result))))))

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
                      ;; No :all. Killing every node at once tests nothing this
                      ;; suite can check: with no surviving replica there is no
                      ;; quorum to acknowledge anything, so the run simply stops
                      ;; producing history until the cluster is restarted. It
                      ;; cost most of a run's operations when it rolled — the
                      ;; 2026-08-13 nightly acknowledged 150 appends under
                      ;; :kill and 87 under :partition,:kill, against 1715 for
                      ;; :partition alone — and a history that thin is closer to
                      ;; :insufficient-data than to evidence. :majority already
                      ;; covers quorum loss while leaving a replica that must
                      ;; still be able to prove what it kept.
                      :kill      {:targets [:one :majority]}
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
                           :stats      (stats-checker)
                           :exceptions (checker/unhandled-exceptions)
                           :workload   (:checker workload)})
            :generator  (gen/phases
                          (->> (:generator workload)
                               (gen/stagger (/ (:rate opts 15)))
                               (gen/nemesis (:generator nemesis))
                               (gen/time-limit (:time-limit opts)))
                          (gen/log "Healing cluster")
                          (gen/nemesis (:final-generator nemesis))
                          ;; The final read compares replicas, so taking it
                          ;; while one is still catching up reports catch-up
                          ;; latency as a durability violation. This used to be
                          ;; a fixed sleep, which can only ever be tuned against
                          ;; one machine: 90 s was measured on a 4-CPU Docker
                          ;; Desktop VM and was not enough on a hosted CI
                          ;; runner, where it turned every log-append job red
                          ;; with losses that were only lag.
                          ;;
                          ;; The wait now ends on the condition itself — every
                          ;; reporting node caught up to the acknowledged
                          ;; high-water mark — with --recovery-time demoted to
                          ;; the deadline. Faster than the old constant when the
                          ;; cluster recovers quickly, and safe when it does
                          ;; not, because a run that hits the deadline records
                          ;; that it did instead of quietly reading anyway.
                          ;;
                          ;; Workloads with no final read supply no
                          ;; :await-generator and skip this entirely rather than
                          ;; sleeping through it for nothing.
                          (gen/log "Waiting for replicas to converge")
                          (gen/clients (:await-generator workload))
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

   [nil "--recovery-time SECONDS" "Deadline, in seconds, on the wait for
                                  replicas to catch up after healing. The wait
                                  ends as soon as every reporting node reaches
                                  the acknowledged high-water mark, so this is
                                  a ceiling and not a duration — raising it
                                  costs nothing on a healthy run. A run that
                                  hits it reports :converged? false, which is
                                  what makes a tail loss unproven rather than a
                                  finding."
    :default 90
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--poll-interval SECONDS" "Seconds between frontier polls during the
                                  recovery wait."
    :default 2
    :parse-fn read-string
    :validate [pos? "must be positive"]]

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
