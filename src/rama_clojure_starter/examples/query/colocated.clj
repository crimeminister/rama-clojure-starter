(ns rama-clojure-starter.examples.query.colocated
  "Port of rama.examples.query.ColocatedQueryTopologyInvokeModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule ColocatedQueryTopologyInvokeModule [setup topologies]
  ;; NB: query topologies are implicitly batch blocks.

  (<<query-topology topologies "q1"
   [*a :> *res]
   ;; Return triple the value of *a.
   (* *a 3 :> *res)
   ;; Every query topology must have an |origin call, which indicates to
   ;; move the computation back to where the query started.
   (|origin))

  (<<query-topology topologies "q2"
   [*a :> *res]
   ;; Invoke query q1 and bind the result to *v.
   (invoke-query "q1" *a :> *v)
   ;; Return one more than *v.
   (+ *v 1 :> *res)
   ;; Every query topology must have an |origin call, which indicates to
   ;; move the computation back to where the query started.
   (|origin)))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc ColocatedQueryTopologyInvokeModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name ColocatedQueryTopologyInvokeModule)
          ;; query
          q2 (foreign-query ipc module-name "q2")]

      (println "Query 1:" (foreign-invoke-query q2 1))
      (println "Query 2:" (foreign-invoke-query q2 10)))))
