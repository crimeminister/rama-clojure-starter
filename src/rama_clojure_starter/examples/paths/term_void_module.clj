(ns rama-clojure-starter.examples.paths.term-void-module
  "Port of rama.examples.paths.TermVoidModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

(defmodule TermVoidModule [setup topologies]
  ;; Depots
  (declare-depot setup *init-depot :random)
  (declare-depot setup *term-void-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition
    ;; on task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *init-depot
      ;;
      (source> *init-depot)

      ;; Set initial state: {"a" (1 2 3), "b" "xyz"}
      (local-transform> [(keypath "a") AFTER-ELEM (termval 1)] $$p)
      (local-transform> [(keypath "a") AFTER-ELEM (termval 2)] $$p)
      (local-transform> [(keypath "a") AFTER-ELEM (termval 3)] $$p)
      (local-transform> [(keypath "b") (termval 1)] $$p)

      ;; *term-void-depot
      (source> *term-void-depot)

      ;; NONE is a special value that can be set to indicate a navigator
      ;; should perform a removal of the key leading to that value.
      ;;
      ;; NB: in Rama dataflow code, NONE> should be used instead
      ;; of (termval NONE).

      ;; {"a" (1 3), "b" "xyz"}
      (local-transform> [(keypath "a") (nthpath 1) NONE>] $$p)
      ;; {"a" (1 2 3)}
      (local-transform> [(keypath "b") NONE>] $$p))))

;; EXPECTED OUTPUT
;;
;; Initial value: {a (1 2 3), b 1}
;; After transform: {a (1 3)}

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc TermVoidModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name TermVoidModule)
          ;; depots
          init-depot (foreign-depot ipc module-name "*init-depot")
          term-void-depot (foreign-depot ipc module-name "*term-void-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! init-depot nil)
      (println "Initial value:" (foreign-select-one STAY pstate))

      (foreign-append! term-void-depot nil)
      (println "After transform:" (foreign-select-one STAY pstate)))))
