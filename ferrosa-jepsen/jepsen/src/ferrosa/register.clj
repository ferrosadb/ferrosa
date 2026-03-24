(ns ferrosa.register
  (:require [jepsen.tests :as tests]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.client :as client]
            [knossos.model :as model]
            [ferrosa.client :as fc]
            [ferrosa.db :as fdb]
            [ferrosa.nemesis :as fn]
            [clojure.tools.logging :refer [info]]))

(defrecord RegisterClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (fc/connect [node])))

  (setup! [this test]
    (fc/setup-register-table! conn))

  (invoke! [this test op]
    (case (:f op)
      :read  (let [val (fc/read-register conn)]
               (assoc op :type :ok :value val))
      :write (do (fc/write-register! conn (:value op))
                 (assoc op :type :ok))
      :cas   (let [[expected new-val] (:value op)
                   [applied? _] (fc/cas-register! conn expected new-val)]
               (assoc op :type (if applied? :ok :fail)))))

  (teardown! [this test])

  (close! [this test]
    (fc/disconnect conn)))

(defn register-test
  "Jepsen test for linearizable register with Knossos checker."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "ferrosa-register"
          :db         (fdb/db)
          :client     (RegisterClient. nil)
          :nemesis    (fn/partition-halves-nemesis)
          :checker    (checker/compose
                       {:linear   (checker/linearizable
                                   {:model (model/cas-register)
                                    :algorithm :wgl})
                        :timeline (timeline/html)})
          :generator  (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [{:f :read}
                                            {:f :write :value (rand-int 100)}
                                            {:f :cas :value [(rand-int 100) (rand-int 100)]}])
                                   (gen/stagger 1/50))))
                           (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info :f :start}
                                    (gen/sleep 10)
                                    {:type :info :f :stop}]))
                           (gen/time-limit (:time-limit opts 60)))}))
