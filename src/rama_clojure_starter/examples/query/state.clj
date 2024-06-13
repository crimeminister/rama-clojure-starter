(ns rama-clojure-starter.examples.query.state
  "Port of rama.examples.query.TemporaryQueryStateModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule TemporaryQueryStateModule [setup topologies]
  ;; Compute the sum of two numbers, along the way demonstrating how a
  ;; PState may be used for temporary storage during a query topology
  ;; execution.
  ;;
  ;; NB: query topologies are implicitly batch blocks.
  (<<query-topology topologies "foo"
   [*v *v2 :> *res]
   ;;
   (|hash *v)
   ;; Make use of local state by storing the value of *v2 in a local
   ;; PState.  local-transform> transforms the PState partition on
   ;; current task with the given path. Transform path must use term,
   ;; termval, or NONE> at leaves.
   (local-transform> (termval *v2) $$foo$$)
   ;; Partitions to a single task chosen in random round robin
   ;; order. Once one event has gone to each task, re-randomizes order
   ;; again.
   (|shuffle)
   ;; Return to the partition where our temporary state is stored.
   (|hash *v)
   ;; Retrieve the value of *v2 stored earlier on in a temporary PState.
   (local-select> STAY $$foo$$ :> *v3)
   ;; Every query topology must have an |origin call, which indicates to
   ;; move the computation back to where the query started.
   (|origin)
   ;; Return the sum of *v and *v2, where *v2 was retrieved from
   ;; temporary storage.
   (+ *v *v3 :> *res)))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc TemporaryQueryStateModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name TemporaryQueryStateModule)
          ;; query
          foo (foreign-query ipc module-name "foo")]

      (println "Result:" (foreign-invoke-query foo 2 3))
      (println "Result:" (foreign-invoke-query foo 2 4))
      (println "Result:" (foreign-invoke-query foo 3 5)))))
