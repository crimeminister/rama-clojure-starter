(ns rama-clojure-starter.examples.query.fibonacci
  "Port of rama.examples.query.FibonacciModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule FibonacciModule [setup topologies]
  (<<query-topology topologies "fib"
   [*n :> *res]
   (<<if (or> (= *n 0) (= *n 1))
     (identity 1 :> *res)
   (else>)
     (invoke-query "fib" (dec *n) :> *a)
     (invoke-query "fib" (- *n 2) :> *b)
     (+ *a *b :> *res))
   ;; Every query topology must have an |origin call, which indicates to
   ;; move the computation back to where the query started.
   (|origin)))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc FibonacciModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name FibonacciModule)
          ;; query
          fib (foreign-query ipc module-name "fib")]

      (doseq [i (range 10)]
        (println "Fib(" i "):" (foreign-invoke-query fib i))))))
