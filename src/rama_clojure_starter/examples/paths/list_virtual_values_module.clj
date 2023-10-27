(ns rama-clojure-starter.examples.paths.list-virtual-values-module
  "Port of rama.examples.paths.ListVirtualValuesModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule ListVirtualValuesModule [setup topologies]
  ;; Depots
  (declare-depot setup *append-depot :random)
  (declare-depot setup *prepend-depot :random)
  (declare-depot setup *insert-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition
    ;; on task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *append-depot
      ;;
      (source> *append-depot :> *v)
      (local-transform> [AFTER-ELEM (termval *v)] $$p)

      ;; *prepend-depot
      ;;
      (source> *prepend-depot :> *v)
      (local-transform> [BEFORE-ELEM (termval *v)] $$p)

      ;; *insert-depot
      ;;
      (source> *insert-depot :> *v)
      (local-transform> [(before-index 2) (termval *v)] $$p))))

;; EXPECTED OUTPUT
;;
;; After appends: [a b c]
;; After prepend: [d a b c]
;; After insert: [d a e b c]

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc ListVirtualValuesModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name ListVirtualValuesModule)
          ;; depots
          append-depot (foreign-depot ipc module-name "*append-depot")
          prepend-depot (foreign-depot ipc module-name "*prepend-depot")
          insert-depot (foreign-depot ipc module-name "*insert-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! append-depot "a")
      (foreign-append! append-depot "b")
      (foreign-append! append-depot "c")
      (println "After appends:" (foreign-select ALL pstate))

      (foreign-append! prepend-depot "d")
      (println "After prepend:" (foreign-select ALL pstate))

      (foreign-append! insert-depot "e")
      (println "After insert:" (foreign-select ALL pstate)))))
