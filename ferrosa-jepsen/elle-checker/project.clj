(defproject ferrosa-elle-check "0.1.0"
  :description "Standalone Elle strict-serializability checker for the Ferrosa
                Accord list-append history EDN (t_d7ffb5b7). Isolated from the
                main jepsen harness so it depends only on jepsen (which bundles
                Elle), avoiding that project's unrelated CQL-driver deps."
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.6"]]
  ;; Run headless: Elle/jepsen pulls in AWT, which throws HeadlessException on a
  ;; display-less host (CI). The verdict is printed to stdout; no graphs needed.
  :jvm-opts ["-Djava.awt.headless=true"]
  :main ferrosa.elle-check)
