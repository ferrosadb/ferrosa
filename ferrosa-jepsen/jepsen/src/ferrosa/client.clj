(ns ferrosa.client
  (:require [qbits.alia :as alia]
            [qbits.hayt :as h]
            [clojure.tools.logging :refer [info warn]]))

(defn connect
  "Connect to ferrosa cluster. Returns a session."
  [nodes]
  (let [cluster (alia/cluster {:contact-points nodes
                               :load-balancing-policy :round-robin})
        session (alia/connect cluster)]
    {:cluster cluster :session session}))

(defn disconnect
  "Disconnect from cluster."
  [{:keys [cluster session]}]
  (when session (alia/shutdown session))
  (when cluster (alia/shutdown cluster)))

(defn execute
  "Execute a CQL query, returning rows."
  [{:keys [session]} query]
  (alia/execute session query))

(defn execute-with-serial
  "Execute with SERIAL consistency."
  [{:keys [session]} query]
  (alia/execute session query {:serial-consistency :serial}))

(defn setup-register-table!
  "Create the register test table."
  [conn]
  (execute conn (h/create-keyspace :jepsen
                  (h/if-not-exists)
                  (h/with {:replication {"class" "SimpleStrategy"
                                         "replication_factor" 3}})))
  (execute conn (h/create-table :jepsen.register
                  (h/if-not-exists)
                  (h/column-definitions {:id :int :val :int :primary-key :id})))
  (execute conn (h/insert :jepsen.register (h/values {:id 0 :val 0}))))

(defn setup-bank-table!
  "Create the bank test table with 10 accounts."
  [conn]
  (execute conn (h/create-keyspace :jepsen
                  (h/if-not-exists)
                  (h/with {:replication {"class" "SimpleStrategy"
                                         "replication_factor" 3}})))
  (execute conn (h/create-table :jepsen.accounts
                  (h/if-not-exists)
                  (h/column-definitions {:id :int :balance :bigint :primary-key :id})))
  (doseq [i (range 10)]
    (execute conn (h/insert :jepsen.accounts (h/values {:id i :balance 1000})))))

(defn read-register
  "Read the current register value."
  [conn]
  (-> (execute conn (h/select :jepsen.register (h/where {:id 0})))
      first
      :val))

(defn write-register!
  "Write a value to the register."
  [conn val]
  (execute conn (h/update :jepsen.register
                  (h/set-columns {:val val})
                  (h/where {:id 0}))))

(defn cas-register!
  "Compare-and-swap on the register. Returns [applied? current-val]."
  [conn expected new-val]
  (let [result (execute conn
                 (str "UPDATE jepsen.register SET val = " new-val
                      " WHERE id = 0 IF val = " expected))]
    (let [row (first result)]
      [(get row (keyword "[applied]") false)
       (get row :val)])))

(defn read-all-balances
  "Read all account balances."
  [conn]
  (->> (execute conn (h/select :jepsen.accounts))
       (map (fn [r] [(:id r) (:balance r)]))
       (into (sorted-map))))

(defn transfer!
  "Transfer amount from one account to another using LWT."
  [conn from to amount]
  (let [from-bal (:balance (first (execute conn
                    (h/select :jepsen.accounts (h/where {:id from})))))
        to-bal (:balance (first (execute conn
                    (h/select :jepsen.accounts (h/where {:id to})))))]
    (when (and from-bal (>= from-bal amount))
      (execute conn
        (str "UPDATE jepsen.accounts SET balance = " (- from-bal amount)
             " WHERE id = " from " IF balance = " from-bal))
      (execute conn
        (str "UPDATE jepsen.accounts SET balance = " (+ to-bal amount)
             " WHERE id = " to " IF balance = " to-bal)))))
