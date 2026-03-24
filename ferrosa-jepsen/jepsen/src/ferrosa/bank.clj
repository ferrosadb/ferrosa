(ns ferrosa.bank
  (:require [jepsen.tests :as tests]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.client :as client]
            [elle.core :as elle]
            [ferrosa.client :as fc]
            [ferrosa.db :as fdb]
            [ferrosa.nemesis :as fn]
            [clojure.tools.logging :refer [info]]))

(def num-accounts 10)
(def initial-balance 1000)
(def total-balance (* num-accounts initial-balance))

(defrecord BankClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (fc/connect [node])))

  (setup! [this test]
    (fc/setup-bank-table! conn))

  (invoke! [this test op]
    (case (:f op)
      :read     (let [balances (fc/read-all-balances conn)]
                  (assoc op :type :ok :value balances))
      :transfer (let [{:keys [from to amount]} (:value op)]
                  (try
                    (fc/transfer! conn from to amount)
                    (assoc op :type :ok)
                    (catch Exception e
                      (assoc op :type :fail :error (.getMessage e)))))))

  (teardown! [this test])

  (close! [this test]
    (fc/disconnect conn)))

(defn bank-checker
  "Check that total balance is conserved."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [reads (->> history
                       (filter #(= :ok (:type %)))
                       (filter #(= :read (:f %)))
                       (map :value))
            bad-reads (filter #(not= total-balance (reduce + (vals %))) reads)]
        {:valid?     (empty? bad-reads)
         :read-count (count reads)
         :bad-reads  bad-reads}))))

(defn bank-test
  "Jepsen test for bank transfers with balance conservation."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "ferrosa-bank"
          :db         (fdb/db)
          :client     (BankClient. nil)
          :nemesis    (fn/kill-nemesis)
          :checker    (checker/compose
                       {:bank     (bank-checker)
                        :timeline (timeline/html)})
          :generator  (->> (gen/mix [{:f :read}
                                     {:f :transfer
                                      :value {:from   (rand-int num-accounts)
                                              :to     (rand-int num-accounts)
                                              :amount (inc (rand-int 100))}}])
                           (gen/stagger 1/50)
                           (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info :f :start}
                                    (gen/sleep 15)
                                    {:type :info :f :stop}]))
                           (gen/time-limit (:time-limit opts 60)))}))
