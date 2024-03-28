(ns rama-clojure-starter.examples.pstates.partition
  "Port of rama.examples.pstates.CustomKeyPartitionerModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.test :as rtest]))


(defn my-key-partitioner
  [num-partitions key]
  (dec num-partitions))

(defmodule CustomKeyPartitionerModule [setup topologies]
  (let [s (stream-topology topologies "s")]
    ;; The :key-partitioner option specifies a custom function for
    ;; routing foreign PState client queries to partitions. Should be a
    ;; reference to a function of two arguments, num-partitions and key,
    ;; that picks a partition between 0 and num-partitions.
    (declare-pstate s $$p Object {:key-partitioner my-key-partitioner})))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc CustomKeyPartitionerModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name CustomKeyPartitionerModule)
          p (foreign-pstate ipc module-name "$$p")]
      (println "Using custom pstate partitioner"))))
