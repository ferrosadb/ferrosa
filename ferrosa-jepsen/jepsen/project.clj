(defproject ferrosa-jepsen "0.1.0"
  :description "Jepsen tests for Ferrosa distributed database"
  :url "https://github.com/bkearns/ferrosa"
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.6"]
                 [cc.qbits/alia "5.0.0-alpha7"]
                 [cc.qbits/hayt "5.0.1"]
                 [cheshire "5.13.0"]]
  :main ferrosa.runner
  :profiles {:uberjar {:aot :all}})
