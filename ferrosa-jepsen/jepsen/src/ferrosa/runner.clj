(ns ferrosa.runner
  (:require [jepsen.cli :as cli]
            [ferrosa.register :as register]
            [ferrosa.bank :as bank]
            [ferrosa.lwt :as lwt])
  (:gen-class))

(def tests
  {"register" register/register-test
   "bank"     bank/bank-test
   "lwt"      lwt/lwt-test})

(defn -main
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn (fn [opts]
                                           (let [test-name (get opts :test "register")
                                                 test-fn (get tests test-name)]
                                             (if test-fn
                                               (test-fn opts)
                                               (throw (ex-info (str "Unknown test: " test-name)
                                                               {:test test-name
                                                                :available (keys tests)})))))})
          (cli/serve-cmd))))
