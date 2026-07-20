(ns ferrosa.elle-check
  "Standalone Elle strict-serializability check over a `list-append` history
   EDN produced by the Rust `elle_list_append` generator (t_d7ffb5b7).

   The generator writes Elle-native ops:
     {:process P :type :invoke|:ok|:info|:fail :f :txn
      :value [[:append k v]] | [[:r k [v ...]]]}
   We index/pair them into a jepsen history and run
   `elle.list-append/check` under the :strict-serializable model — the
   consistency Accord claims to provide.

   Usage: lein run -m ferrosa.elle-check <history.edn> [out-dir]"
  (:require [clojure.edn :as edn]
            [jepsen.history :as history]
            [elle.list-append :as la])
  (:gen-class))

(defn -main
  [& args]
  (let [path    (or (first args) "elle-history.edn")
        ;; A directory triggers Elle's graphviz/gnuplot rendering of anomaly
        ;; graphs. Those external tools are optional; only pass a directory when
        ;; the caller explicitly asks (2nd arg), so the verdict is always
        ;; reported even without `dot`/`gnuplot` installed.
        out-dir (second args)
        raw     (edn/read-string (slurp path))
        h       (history/history raw)
        opts    (cond-> {:consistency-models [:strict-serializable]}
                  out-dir (assoc :directory out-dir))
        res     (la/check opts h)
        valid   (:valid? res)]
    (println "=== Elle list-append / strict-serializable ===")
    (println "history:" path)
    (println "ops:" (count raw))
    (println "valid?" valid)
    (println "anomaly-types:" (:anomaly-types res))
    (when (seq (:anomaly-types res))
      (println "anomaly counts:"
               (into (sorted-map)
                     (map (fn [[k v]] [k (if (coll? v) (count v) v)])
                          (:anomalies res)))))
    (println "output dir:" out-dir)
    (flush)
    (shutdown-agents)
    ;; Exit 0 ONLY on a definitive :valid? true. false = real violation;
    ;; :unknown = indeterminate (e.g. too many :info ops) — both non-zero so
    ;; CI can't mistake an unproven run for a pass (fail-loud).
    (System/exit (if (true? valid) 0 1))))
