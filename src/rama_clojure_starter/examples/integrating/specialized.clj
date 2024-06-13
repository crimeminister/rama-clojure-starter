(ns rama-clojure-starter.examples.integrating.specialized
  "Port of rama.examples.integrating.SpecializedTaskGlobalModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest])
  (:import
   [com.rpl.rama.integration TaskGlobalContext TaskGlobalObject]))

;; A task global allows global state to be available to all tasks of a
;; module. Task globals live on each task and do not need to be
;; transferred from task to task during processing. Task globals can be
;; constant values, or each instance of a task global can specialize
;; itself to each task. Task globals can be used for everything from
;; distributing large constant data (like an ML model), in-memory caches
;; specific to each partition, or integrating external services like
;; databases, queues, or monitoring systems (where the task global opens
;; up a client to the external service on each task).

(definterface SpecialTask
  (^long getSpecialId []))

(deftype MyTaskGlobal [v ^:volatile-mutable special]
  ;; If the provided object implements TaskGlobalObject, each instance
  ;; will be specialized on each task.
  TaskGlobalObject
  ;; Specialized this object for this particular task ID. This is called
  ;; during worker launch. Each task has a separate instance of this
  ;; object by using de/serialization.
  (prepareForTask [this task-id context]
    (let [;; Returns information about the current module (as a
          ;; ModuleInstanceInfo).
          module-info (.getModuleInstanceInfo context)
          ;; Returns replica ID of this task thread.
          replica-id (.getReplicaId context)
          ;; Return a set of all task IDs in the task group where this
          ;; task global is being prepared.
          task-group (.getTaskGroup context)
          ;; Returns the var name assigned to this object in the module
          ;; definition.
          var-name (.getVarName context)]
      (set! special (* task-id v))))
  ;; Called on this object whenever the task on which it lives gains
  ;; leadership.
  (gainedLeadership [this]
    )
  ;; Clean up any resources opened by the task global when the worker is
  ;; being shut down, e.g. database connections.
  (close [this]
    )

  SpecialTask
  (getSpecialId [this]
    special))

;; Note that "You can’t do Java interop in Rama code, e.g. (.size
;; l). Instead, you should make a wrapper function in Clojure and call
;; that wrapper function from Rama."
(defn get-special-id [^MyTaskGlobal mtg]
  (.getSpecialId mtg))

(defmodule SpecializedTaskGlobalModule [setup topologies]
  ;; Objects
  (declare-object setup *tg (->MyTaskGlobal 10 nil))
  ;; Depots
  (declare-depot setup *depot :random)

  (let [s (stream-topology topologies "s")]
    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *depot)
     ;; Store the current task ID.
     (ops/current-task-id :> *task-id-1)
     ;; Call wrapper fn to get the "special" global ID.
     (get-special-id *tg :> *special-1)

     (|shuffle)

     (ops/current-task-id :> *task-id-2)
     ;; Call wrapper fn to get the "special" global ID.
     (get-special-id *tg :> *special-2)

     (println "Results:" *task-id-1 "->" *special-1 "," *task-id-2 "->" *special-2))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc SpecializedTaskGlobalModule {:tasks 8 :threads 4})
    (let [module-name (get-module-name SpecializedTaskGlobalModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")]

      (foreign-append! depot nil))))
