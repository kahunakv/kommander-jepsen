(defproject kommander-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Kommander, a partitioned Raft consensus library for .NET"
  :url "https://github.com/kahunakv/kommander"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :main kommander.core
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [jepsen "0.3.7"]
                 [clj-http "3.13.0"]
                 [cheshire "5.13.0"]
                 [slingshot "0.12.2"]]
  ;; Knossos's linearizability search is the memory bottleneck, not the test
  ;; run: a history with many indeterminate (:info) ops explodes the search
  ;; space and will OOM with the default heap. Raise this (and lower
  ;; --ops-per-key) if you see {:valid? :unknown, :cause :out-of-memory}.
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             ;; Sized for a 4 GB Docker Desktop VM. A heap larger than the
             ;; machine gets the JVM OOM-killed by the kernel with no Java
             ;; stack trace at all, which looks like a mysterious hang.
             "-Xmx2g"]
  ;; CI runners have far more headroom than a Docker Desktop VM, so the
  ;; analysis can afford a real heap. Used by .github/workflows/jepsen.yml via
  ;; `lein with-profile +ci run test ...`. Independent keys are analyzed
  ;; *concurrently* by an agent pool, so several Knossos searches compete for
  ;; the heap at once — both this and --ops-per-key matter.
  :profiles {:ci {:jvm-opts ["-Djava.awt.headless=true" "-server" "-Xmx11g"]}}

  :repl-options {:init-ns kommander.core})
