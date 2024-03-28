(ns rama-clojure-starter.examples.pstates.client-select
  "Port of rama.examples.pstates.ClientBasicSelectExamplesModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule ClientBasicSelectExamplesModule [setup topologies]
  ;; Depots
  (declare-depot setup *depot (hash-by first))

  (let [s (stream-topology topologies "s")]
    (declare-pstate s $$p {String (set-schema Long)})

    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *depot :> *tuple)
     ;; Emits every element of input list as a separate field. Useful
     ;; for processing elements of fixed-size lists.
     (ops/expand *tuple :> *k *v)
     ;; Increment the count of [*k1 *k2] pairs seen.
     (+compound $$p {*k (aggs/+set-agg *v)}))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc ClientBasicSelectExamplesModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name ClientBasicSelectExamplesModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")
          ;; pstates
          p (foreign-pstate ipc module-name "$$p")]

      (foreign-append! depot ["cagney" 1])
      (foreign-append! depot ["cagney" 7])
      (foreign-append! depot ["cagney" 3])
      (foreign-append! depot ["cagney" 8])

      (foreign-append! depot ["davis" 10])
      (foreign-append! depot ["davis" 12])
      (foreign-append! depot ["davis" 14])

      (println "select (davis):" (foreign-select (keypath "davis") p))
      (println "select-one (davis):" (foreign-select-one (keypath "davis") p))
      (println "select (cagney):" (foreign-select [(keypath "cagney") ALL (pred odd?)] p)))))
