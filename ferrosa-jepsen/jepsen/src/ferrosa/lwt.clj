(ns ferrosa.lwt
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

;; Simplified: tests INSERT IF NOT EXISTS + UPDATE IF patterns
;; covering the core LWT consistency guarantees

(defrecord LwtClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (fc/connect [node])))

  (setup! [this test]
    (fc/execute conn
      "CREATE KEYSPACE IF NOT EXISTS jepsen WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
    (fc/execute conn
      "CREATE TABLE IF NOT EXISTS jepsen.lwt (id int PRIMARY KEY, val int, version int)")
    (fc/execute conn
      "INSERT INTO jepsen.lwt (id, val, version) VALUES (0, 0, 0)"))

  (invoke! [this test op]
    (try
      (case (:f op)
        :read (let [rows (fc/execute conn
                    "SELECT val, version FROM jepsen.lwt WHERE id = 0")
                    row (first rows)]
                (assoc op :type :ok :value [(:val row) (:version row)]))

        :write (let [[new-val expected-ver] (:value op)
                     result (fc/execute conn
                              (str "UPDATE jepsen.lwt SET val = " new-val
                                   ", version = " (inc expected-ver)
                                   " WHERE id = 0 IF version = " expected-ver))
                     applied? (get (first result) (keyword "[applied]") false)]
                 (assoc op :type (if applied? :ok :fail)
                           :value [new-val expected-ver applied?]))

        :insert (let [id (:value op)
                      result (fc/execute conn
                               (str "INSERT INTO jepsen.lwt (id, val, version) "
                                    "VALUES (" id ", 0, 0) IF NOT EXISTS"))
                      applied? (get (first result) (keyword "[applied]") false)]
                  (assoc op :type (if applied? :ok :fail))))
      (catch Exception e
        (assoc op :type :info :error (.getMessage e)))))

  (teardown! [this test])

  (close! [this test]
    (fc/disconnect conn)))

(defn lwt-checker
  "Check LWT consistency: no two successful writes at the same version."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [writes (->> history
                        (filter #(= :ok (:type %)))
                        (filter #(= :write (:f %)))
                        (map :value))
            ;; Group by expected version, check at most one success per version
            by-version (group-by second writes)
            conflicts (filter (fn [[ver ops]] (> (count ops) 1)) by-version)]
        {:valid?    (empty? conflicts)
         :writes    (count writes)
         :conflicts conflicts}))))

(defn lwt-test
  "Jepsen test for LWT consistency with Elle."
  [opts]
  (merge tests/noop-test
         opts
         {:name       "ferrosa-lwt"
          :db         (fdb/db)
          :client     (LwtClient. nil)
          :nemesis    (fn/partition-halves-nemesis)
          :checker    (checker/compose
                       {:lwt      (lwt-checker)
                        :timeline (timeline/html)})
          :generator  (->> (gen/mix [{:f :read}
                                     {:f :write :value [(rand-int 1000) (rand-int 100)]}
                                     {:f :insert :value (rand-int 10000)}])
                           (gen/stagger 1/50)
                           (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info :f :start}
                                    (gen/sleep 10)
                                    {:type :info :f :stop}]))
                           (gen/time-limit (:time-limit opts 60)))}))
