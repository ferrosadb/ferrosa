(ns ferrosa.nemesis
  (:require [jepsen.nemesis :as nemesis]
            [jepsen.control :as c]
            [jepsen.util :as util]
            [clojure.tools.logging :refer [info]]))

(defn partition-halves-nemesis
  "Partition cluster into majority/minority using iptables."
  []
  (nemesis/partitioner (comp nemesis/complete-grudge nemesis/bisect)))

(defn kill-nemesis
  "Kill ferrosa on random nodes."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :start (let [nodes (util/random-nonempty-subset (:nodes test))]
                 (doseq [node nodes]
                   (c/on node
                     (c/exec :pkill :-9 :-f "ferrosa" :|| :true)))
                 (assoc op :value (str "killed " nodes)))
        :stop  (let [seeds (clojure.string/join "," (:nodes test))]
                 (doseq [node (:nodes test)]
                   (c/on node
                     (try
                       (c/exec :pgrep :-f "ferrosa")
                       (catch Exception _
                         (c/exec :nohup :ferrosa
                                 :--seeds seeds
                                 :--listen node
                                 :> "/var/log/ferrosa.log" :2>&1 :&)))))
                 (assoc op :value "restarted"))))
    (teardown! [this test])))

(defn clock-nemesis
  "Small clock skew via faketime."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :start (do (doseq [node (:nodes test)]
                     (let [skew (/ (- (rand-int 1000) 500) 1000.0)]
                       (c/on node
                         (c/exec :echo (str "FAKETIME=\"" (if (pos? skew) "+") skew "\"")
                                 :> "/etc/faketime.conf"))))
                   (assoc op :value "skewed"))
        :stop  (do (doseq [node (:nodes test)]
                     (c/on node
                       (c/exec :rm :-f "/etc/faketime.conf")))
                   (assoc op :value "unskewed"))))
    (teardown! [this test])))
