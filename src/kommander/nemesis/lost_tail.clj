(ns kommander.nemesis.lost-tail
  "One directed scenario: a voter loses acknowledged entries from its disk, and
  then votes without them. This is the failure in Alagappan et al., \"Protocol-
  Aware Recovery for Consensus-Based Storage\" (FAST 2018).

  ## The sequence, for five nodes

    1. :lost-tail-isolate  Cut {d e} from {a b v}. A commit now needs a, b and v,
                           so every acknowledged entry is on the victim v.
                           Record the size of v's live log.
    2. (window)            Clients append. The cluster acknowledges entries that
                           d and e do not have.
    3. :lost-tail-damage   Record v's watermarks. SIGKILL v. Truncate v's live log
                           to the recorded size.
    4. :lost-tail-restart  Cut v from {a b} too, so v has no peers. Start v. Read
                           its verdict. No peer can change the reading.
    5. :lost-tail-rejoin   Connect v to {d e}. {v d e} is a majority without the
                           entries from step 2. A quarantined v refuses to vote,
                           so this side stays leaderless, and the entries are
                           safe. A v that votes lets this side elect a leader,
                           commit other entries at those indices, and overwrite
                           the acknowledged entries after the heal.
    6. :lost-tail-heal     Heal the network.

  ## Why a real file truncation

  Kommander's own tests raise the watermark through the API after they remove the
  rows. A real truncation also removes the watermark, because the rows and the
  watermark share one write batch in one log. See Vorpal a516525b. This suite has
  to create the damage that the disk creates.

  ## Why the rules change one edge at a time

  `net/heal!` flushes every rule. A heal between two grudges connects {a b} to
  {d e} for a moment, which is enough to replicate the entries this test needs to
  keep from them. Step 4 therefore adds rules, and step 5 deletes only the rules
  between v and {d e}.

  ## What the size cut can miss

  A rotated log means that part of the window is in a closed log, which the
  MANIFEST check protects. The live log is then cut to 0 and the node loses only
  the part in the live log. The history records :position-at-isolate and
  :position-at-kill, and a run that lost nothing is :unknown.

  The verdict comes from two checkers. `kommander.storage` reports a loss that
  the victim did not detect. The log-append checker reports what the loss did to
  acknowledged entries."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]]
            [jepsen.control.net :as cn]
            [kommander.client :as kc]
            [kommander.db :as kdb]))

(def ^:private warmup-s
  "Load before the first fault, so the live log holds a normal history."
  15)

(def ^:private window-s
  "How long the cluster acknowledges entries that the far side does not have. This
  includes the elections for the partitions that lost their leader in step 1."
  20)

(def ^:private verdict-timeout-s
  "How long to wait for a restarted victim to finish its restore."
  60)

(defn plan
  "Splits `nodes` into the victim and two sides.

  The victim and :old-side make a bare majority, so a commit needs the victim. The
  victim and :new-side also make a bare majority, so the victim decides an
  election. This needs an odd cluster of at least three nodes."
  [nodes]
  (let [size (count nodes)]
    (when (or (even? size) (< size 3))
      (throw (IllegalArgumentException.
               (str "lost-tail needs an odd cluster of at least 3 nodes, not " size))))
    (let [[victim & others] nodes
          quorum            (inc (quot size 2))
          [old-side new-side] (split-at (dec quorum) others)]
      {:victim   victim
       :old-side (vec old-side)
       :new-side (vec new-side)})))

(defn cut-size
  "The size to truncate the live log to. The whole window is above the recorded
  size when the file is the same. After a rotation, the live log holds only
  entries from the window."
  [at-isolate at-kill]
  (if (= (:file at-isolate) (:file at-kill))
    (min (:size at-isolate) (:size at-kill))
    0))

(defn settled?
  "Did every partition of a /debug/storage response reach a verdict?"
  [response]
  (and (= "ok" (:status response))
       (seq (:partitions response))
       (not-any? (comp #{"Restoring" "not-hosted"} :state) (:partitions response))))

(defn- on-node
  [test node f]
  (get (c/on-nodes test [node] (fn [_ _] (f))) node))

(defn- live-log
  "The newest RocksDB log file on this node, and its size."
  []
  (c/su
    (let [file (c/exec :bash :-c (str "ls -1 " kdb/engine-dir "/*.log | sort | tail -n 1"))]
      {:file file
       :size (Long/parseLong (c/exec :stat :-c "%s" file))})))

(defn- read-storage
  "A connection failure is a result here: the node can be down or can refuse to
  open."
  [node]
  (try (kc/storage node)
       (catch Exception e
         {:status :unreachable :error (.getMessage e)})))

(defn- await-verdict
  [node]
  (let [deadline (+ (System/nanoTime) (* verdict-timeout-s 1000000000))]
    (loop []
      (let [response (read-storage node)]
        (if (or (settled? response) (<= deadline (System/nanoTime)))
          response
          (do (Thread/sleep 250)
              (recur)))))))

(defn- allow!
  "Deletes the DROP rules between `node` and `peers`, in both directions. Other
  rules stay, so no other pair of nodes can connect."
  [test node peers]
  (c/on-nodes test (cons node peers)
              (fn [_ current]
                (doseq [peer (if (= current node) peers [node])]
                  (c/su (c/exec :iptables :-D :INPUT :-s (cn/ip peer) :-j :DROP :-w))))))

(defn- isolate!
  [test {:keys [victim old-side new-side] :as a-plan} state]
  (net/drop-all! test (n/complete-grudge [(cons victim old-side) new-side]))
  (let [position (on-node test victim live-log)]
    (swap! state assoc :position-at-isolate position)
    (assoc a-plan :position-at-isolate position)))

(defn- damage!
  [test {:keys [victim]} state]
  (let [before     (read-storage victim)
        watermarks (into {} (map (juxt :partition :watermark)) (:partitions before))]
    (on-node test victim kdb/kill-stragglers!)
    (swap! state assoc :down? true)
    (let [at-kill (on-node test victim live-log)
          target  (cut-size (:position-at-isolate @state) at-kill)]
      (on-node test victim #(c/su (c/exec :truncate :-s target (:file at-kill))))
      (info "lost-tail: cut" (:file at-kill) "on" victim "from" (:size at-kill) "to" target)
      (swap! state assoc :watermarks watermarks :position-at-kill at-kill :cut-to target)
      {:victim victim :watermarks watermarks :position-at-kill at-kill :cut-to target})))

(defn- restart!
  [test {:keys [victim old-side]} state]
  (net/drop-all! test (n/complete-grudge [[victim] old-side]))
  (c/on-nodes test [victim] (fn [test node] (kdb/start! test node)))
  (swap! state assoc :down? false)
  (let [storage (await-verdict victim)]
    (when-not (settled? storage)
      (warn "lost-tail: no verdict from" victim "within" verdict-timeout-s "s:" storage))
    (assoc (select-keys @state [:position-at-isolate :position-at-kill :cut-to :watermarks])
           :victim  victim
           :storage storage)))

(defn- rejoin!
  [test {:keys [victim new-side]}]
  (allow! test victim new-side)
  {:connected (vec (cons victim new-side))})

(defn- heal!
  "Safe at any step. The time limit can end the sequence with the victim down."
  [test {:keys [victim]} state]
  (net/heal! (:net test) test)
  (when (:down? @state)
    (c/on-nodes test [victim] (fn [test node] (kdb/start! test node)))
    (swap! state assoc :down? false))
  :healed)

(defn nemesis
  []
  (let [state (atom nil)]
    (reify n/Nemesis
      (setup! [this test]
        (net/heal! (:net test) test)
        (reset! state {:plan (plan (shuffle (:nodes test)))})
        this)

      (invoke! [_ test op]
        (let [a-plan (:plan @state)]
          (assoc op :value
                 (case (:f op)
                   :lost-tail-isolate (isolate! test a-plan state)
                   :lost-tail-damage  (damage! test a-plan state)
                   :lost-tail-restart (restart! test a-plan state)
                   :lost-tail-rejoin  (rejoin! test a-plan)
                   :lost-tail-heal    (heal! test a-plan state)))))

      (teardown! [_ test]
        (net/heal! (:net test) test))

      n/Reflection
      (fs [_] #{:lost-tail-isolate :lost-tail-damage :lost-tail-restart
                :lost-tail-rejoin :lost-tail-heal}))))

(defn package
  "A nemesis package shaped like the ones jepsen.nemesis.combined returns. Returns
  the no-op package unless :lost-tail is in :faults."
  [opts]
  (if-not (some #{:lost-tail} (:faults opts))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    {:generator       [(gen/sleep warmup-s)
                       {:type :info, :f :lost-tail-isolate}
                       (gen/sleep window-s)
                       {:type :info, :f :lost-tail-damage}
                       {:type :info, :f :lost-tail-restart}
                       {:type :info, :f :lost-tail-rejoin}]
     :final-generator {:type :info, :f :lost-tail-heal}
     :nemesis         (nemesis)
     :perf            #{{:name  "lost-tail"
                         :start #{:lost-tail-isolate}
                         :stop  #{:lost-tail-heal}
                         :color "#E9A0B8"}}}))
