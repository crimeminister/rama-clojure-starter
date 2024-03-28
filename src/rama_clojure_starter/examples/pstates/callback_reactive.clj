(ns rama-clojure-starter.examples.pstates.callback-reactive
  "Port of rama.examples.pstates.CallbackReactiveModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule CallbackReactiveModule [setup topologies]
  ;; Depots
  (declare-depot setup *depot (hash-by first))

  (let [s (stream-topology topologies "s")]
    (declare-pstate s $$p {String {String Long}})

    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *depot :> *tuple)
     ;; Emits every element of input list as a separate field. Useful
     ;; for processing elements of fixed-size lists.
     (ops/expand *tuple :> *k1 *k2)
     ;; Increment the count of [*k1 *k2] pairs seen.
     (+compound $$p {*k1 {*k2 (aggs/+count)}}))))

(defn callback-fn [new-val diff old-val]
  (println "Received callback:" new-val diff old-val))

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc CallbackReactiveModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name CallbackReactiveModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")
          ;; pstates
          p (foreign-pstate ipc module-name "$$p")]

      (foreign-append! depot ["a" "b"])
      (foreign-append! depot ["a" "c"])

      ;; The callback-fn is a function of three arguments: new val,
      ;; diff, old val, that is called whenever a diff is received.
      (let [proxy (foreign-proxy-async (keypath "a") p {:callback-fn callback-fn})]
        (foreign-append! depot ["a" "d"])
        (foreign-append! depot ["a" "c"])
        (Thread/sleep 50)))))
