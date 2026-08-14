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
      ;; :reason is :unknown because this fixture reports no frontier scan — the
      ;; node is visibly behind, but which frontier broke is genuinely not known.
      (is (= [{:node "n2" :partition 1 :applied 5 :log 9 :behind 4 :reason :unknown}]
             (:delivery r))))))

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
;; Which frontier broke
;; ---------------------------------------------------------------------------

(defn- state
  "One node's reported state for one partition, with the frontier fields the
  harness now returns."
  [applied-pairs & {:as frontier}]
  (merge {:entries (entries applied-pairs) :redeliveries 0}
         frontier))

(deftest committed-above-applied-is-an-undelivered-frontier
  (testing "the entries are present, committed and contiguous — they could have
            been delivered and were not, which is the apply path's problem"
    (is (= :undelivered
           (la/classify-frontier
             (state [[1 "a"] [2 "b"]]
                    :log-index 9 :committed-index 9
                    :first-gap-index -1 :first-uncommitted-index -1))))))

(deftest a-present-but-uncommitted-next-entry-is-not-a-delivery-fault
  (testing "withholding here is correct behaviour — the open question is why the
            entry never commits, which is a different subsystem entirely"
    (is (= :uncommitted
           (la/classify-frontier
             (state [[1 "a"] [2 "b"]]
                    :log-index 9 :committed-index 2
                    :first-gap-index -1
                    :first-uncommitted-index 3 :first-uncommitted-type "Proposed"))))))

(deftest a-missing-next-entry-is-a-replication-fault
  (testing "nothing above a gap is deliverable however the apply path behaves;
            only the leader re-shipping it helps"
    (is (= :gap
           (la/classify-frontier
             (state [[1 "a"] [2 "b"]]
                    :log-index 9 :committed-index 2
                    :first-gap-index 3 :first-uncommitted-index -1))))))

(deftest a-gap-outranks-an-uncommitted-entry
  (testing "both can be set; the gap is the lower boundary and the one that has to
            be repaired first"
    (is (= :gap
           (la/classify-frontier
             (state [[1 "a"]]
                    :applied-index 1
                    :log-index 9 :committed-index 1
                    :first-gap-index 2 :first-uncommitted-index 5))))))

(deftest undelivered-outranks-a-gap-above-it
  (testing "entries committed and contiguous below an absent id are deliverable
            right now, with nothing needed from anywhere else; reporting the gap
            instead hides them. This is the n3 p4 case from run 31761087203 —
            applied 171, committed 183, gap at 184 — where 12 entries were sitting
            deliverable and the verdict said :gap"
    (is (= :undelivered
           (la/classify-frontier
             (state [[1 "a"]]
                    :applied-index 171
                    :log-index 199 :committed-index 183
                    :first-gap-index 184 :first-uncommitted-index -1))))))

(deftest the-delivered-frontier-is-used-not-the-workloads-own-maximum
  (testing "the state machine also receives other log types and advances over
            them, so a node can have applied far past this workload's highest
            recorded index without anything being wrong"
    (is (= :caught-up
           (la/classify-frontier
             (state [[3 "a"]]              ; workload's max index is 3 …
                    :applied-index 100     ; … but the node has delivered to 100
                    :log-index 100 :committed-index 100
                    :first-gap-index -1 :first-uncommitted-index -1))))))

(deftest a-node-that-applied-everything-committed-is-caught-up
  (is (= :caught-up
         (la/classify-frontier
           (state [[1 "a"] [2 "b"]]
                  :log-index 2 :committed-index 2
                  :first-gap-index -1 :first-uncommitted-index -1)))))

(deftest a-harness-that-reports-no-frontier-is-unknown-not-guessed
  (testing "an older harness build reports none of these; inventing a reason would
            be exactly the mistake this field exists to prevent"
    (is (= :unknown
           (la/classify-frontier (state [[1 "a"]] :log-index 9))))))

(deftest a-node-serving-entries-its-log-calls-proposed-is-named
  (testing "this node looks caught-up by its own frontier and is the reason every
            other replica shows a gap — the backfill read filters those rows, so it
            ships non-contiguous batches forever. It has to be named, or the verdict
            blames the victims"
    (let [culprit {1 (state [[3 "v1"] [5 "v2"] [9 "v3"]]
                            :applied-index 9
                            :log-index 9 :committed-index 9
                            :first-gap-index -1 :first-uncommitted-index -1
                            :first-proposed-below-applied 4
                            :proposed-below-applied 2)}
          r       (la/check-logs acked {"n1" culprit "n2" clean-node} opts)]
      ;; Its own frontier is fine — which is exactly why it needs a separate field.
      (is (= :caught-up (la/classify-frontier (get culprit 1))))
      (is (= [{:node "n1" :partition 1 :first-proposed 4 :count 2 :applied 9}]
             (:blocking-backfill r))))))

(deftest the-stale-duplicate-guard-reports-zero-when-it-never-fired
  (testing "zero is the informative case: it rules the guard out as an explanation
            for any hole, so it must be a plain 0 and not an absent key"
    (let [r (la/check-logs acked {"n1" clean-node "n2" clean-node} opts)]
      (is (= 0 (:stale-proposed-skipped-since-restart r))))))

(deftest the-stale-duplicate-guard-totals-and-splits-by-node
  (testing "one node counting far more than its peers is a different finding than
            all of them counting a few, so the split has to survive into the verdict"
    (let [noisy {1 (assoc (state [[3 "v1"] [5 "v2"] [9 "v3"]])
                          :stale-proposed-skipped 900)
                 2 (assoc (state []) :stale-proposed-skipped 100)}
          quiet {1 (assoc (state [[3 "v1"] [5 "v2"] [9 "v3"]])
                          :stale-proposed-skipped 4)}
          r     (la/check-logs acked {"n1" noisy "n2" quiet} opts)]
      (is (= {:total 1004 :by-node {"n1" 1000 "n2" 4}}
             (:stale-proposed-skipped-since-restart r))))))

(deftest an-unhosted-partition-is-not-counted-as-zero-skips
  (testing "-1 means the partition is not on this node; summing it would silently
            understate the total and could even make a real count read as zero"
    (let [absent {1 (assoc (state [[3 "v1"] [5 "v2"] [9 "v3"]])
                           :stale-proposed-skipped -1)}
          r      (la/check-logs acked {"n1" absent "n2" clean-node} opts)]
      (is (= 0 (:stale-proposed-skipped-since-restart r))))))

(deftest a-healthy-node-is-not-listed-as-blocking
  (testing "no proposed rows below the frontier means nothing to report; a false
            positive here would point the next investigation at an innocent node"
    (let [r (la/check-logs acked {"n1" clean-node "n2" clean-node} opts)]
      (is (empty? (:blocking-backfill r))))))

(deftest the-verdict-counts-and-explains-each-frontier
  (testing "the summary must name the subsystem, not just the size of the gap"
    (let [held    {1 (state [[3 "v1"] [5 "v2"]]
                            :log-index 9 :committed-index 9
                            :first-gap-index -1 :first-uncommitted-index -1)}
          blocked {1 (state [[3 "v1"] [5 "v2"]]
                            :log-index 9 :committed-index 5
                            :first-gap-index -1
                            :first-uncommitted-index 6 :first-uncommitted-type "Proposed")}
          r       (la/check-logs acked {"n1" held "n2" blocked} opts)
          by-node (into {} (map (juxt :node identity)) (:delivery r))]
      (is (= :undelivered (get-in by-node ["n1" :reason])))
      (is (= :uncommitted (get-in by-node ["n2" :reason])))
      (is (= 6 (get-in by-node ["n2" :first-uncommitted])))
      (is (= "Proposed" (get-in by-node ["n2" :uncommitted-type])))
      (is (= {:undelivered 1 :uncommitted 1} (:frontiers r))))))

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
