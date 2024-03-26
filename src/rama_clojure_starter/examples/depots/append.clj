(ns rama-clojure-starter.examples.depots.append
  "Port of rama.examples.depots.DepotPartitionAppendModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest]))


(defmodule DepotPartitionAppendModule [setup topologies]
  ;; Depots
  (declare-depot setup *incoming-depot (hash-by first))
  (declare-depot setup *outgoing-depot :disallow)

  (let [s (stream-topology topologies "s")]
    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *incoming-depot :> *tuple)
     ;; Emits every element of input list as a separate field. Useful
     ;; for processing elements of fixed-size lists.
     (ops/expand *tuple :> *k *k2 *v)
     ;; Move the computation to a partition determined by the hash of
     ;; the value in *k2.
     (|hash *k2)
     ;; Increment the value in *v and emit the result.
     (inc *v :> *new-tuple)
     ;; Append data to a depot partition from within an ETL with the
     ;; given ack level. Ack level can be :ack, :append-ack, or nil.
     (depot-partition-append! *outgoing-depot *new-tuple :ack))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc DepotPartitionAppendModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name DepotPartitionAppendModule)
          ;; depots
          incoming-depot (foreign-depot ipc module-name "*incoming-depot")
          outgoing-depot (foreign-depot ipc module-name "*outgoing-depot")]

      (foreign-append! incoming-depot ["foo" :a 1])
      (foreign-append! incoming-depot ["bar" :b 2])
      (foreign-append! incoming-depot ["baz" :c 3]))))
