(ns rama-clojure-starter.examples.depots.custom
  "Port of rama.examples.depots.CustomPartitioningModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.test :as rtest]))


(defdepotpartitioner my-partitioner
  [data num-partitions]
  (if (= data 11)
    (dec num-partitions)
    0))

(defmodule CustomPartitioningModule [setup topologies]
  ;; Depots
  (declare-depot setup *depot my-partitioner))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc CustomPartitioningModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name CustomPartitioningModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")]
      (println "Using custom partitioner"))))
