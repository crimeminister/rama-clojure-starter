(ns rama-clojure-starter.examples.pstates.basic-reactive)

(ns rama-clojure-starter.examples.pstates.basic-reactive
  "Port of rama.examples.pstates.BasicReactiveModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule BasicReactiveModule [setup topologies]
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


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc BasicReactiveModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name BasicReactiveModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")
          ;; pstates
          p (foreign-pstate ipc module-name "$$p")]

      (foreign-append! depot ["a" "b"])
      (foreign-append! depot ["a" "c"])

      ;; foreign-proxy performs a reactive query of PState with path
      ;; that navigates to exactly one value. Return is a ProxyState
      ;; whole value can be accessed any time with deref. This
      ;; function blocks until the initial value of the ProxyState
      ;; is known. Server pushes incremental diffs to returned
      ;; ProxyState in the background to maintain its value.
      (let [proxy (foreign-proxy (keypath "a") p)]
        (println "Initial value:" (.get proxy))

        (foreign-append! depot ["a" "d"])
        (Thread/sleep 50)
        (println "New value:" (.get proxy))

        (foreign-append! depot ["a" "c"])
        (Thread/sleep 50)
        (println "New value:" (.get proxy))))))
