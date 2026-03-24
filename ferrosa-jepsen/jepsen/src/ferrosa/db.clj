(ns ferrosa.db
  (:require [jepsen.db :as db]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [clojure.tools.logging :refer [info warn]]))

(def ferrosa-binary "/usr/local/bin/ferrosa")
(def ferrosa-log "/var/log/ferrosa.log")
(def ferrosa-data "/var/lib/ferrosa")

(defn db
  "Ferrosa database for Jepsen. Nodes are managed via SSH."
  []
  (reify db/DB
    (setup! [_ test node]
      (info node "Setting up ferrosa")
      (c/exec :mkdir :-p ferrosa-data)
      ;; Start ferrosa with seeds from test
      (let [seeds (clojure.string/join "," (:nodes test))]
        (cu/start-daemon!
         {:logfile ferrosa-log
          :pidfile "/var/run/ferrosa.pid"
          :chdir ferrosa-data}
         ferrosa-binary
         :--seeds seeds
         :--listen node
         :--data-dir ferrosa-data)))

    (teardown! [_ test node]
      (info node "Tearing down ferrosa")
      (cu/stop-daemon! ferrosa-binary "/var/run/ferrosa.pid")
      (c/exec :rm :-rf ferrosa-data ferrosa-log))

    db/LogFiles
    (log-files [_ test node]
      [ferrosa-log])))
