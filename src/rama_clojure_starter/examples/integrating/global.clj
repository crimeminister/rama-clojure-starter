(ns rama-clojure-starter.examples.integrating.global
  "Port of rama.examples.integrating.BasicTaskGlobalModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest]))


(defmodule BasicTaskGlobalModule [setup topologies]
  ;; Objects
  ;;
  ;; Defines an object to be copied to all tasks. If provided object
  ;; implements TaskGlobalObject, the object will have a specialized
  ;; instance per task. This can be used to integrate external queues,
  ;; databases, or other tools with Rama.
  (declare-object setup *global-value 7)

  ;; Depots
  (declare-depot setup *depot :random)

  (let [s (stream-topology topologies "s")]
    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *depot)
     (println "Task" (ops/current-task-id) "->" *global-value)
     (|shuffle)
     (println "Task" (ops/current-task-id) "->" *global-value))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc BasicTaskGlobalModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name BasicTaskGlobalModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")]

      (foreign-append! depot nil))))
