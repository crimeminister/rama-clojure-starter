(ns rama-clojure-starter.examples.paths.multi-arity-term-module
  "Port of rama.examples.paths.MultiArityTermModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest])
  (:import
   [com.rpl.rama.ops Ops]))


(defmodule MultiArityTermModule [setup topologies]
  ;; Depots
  (declare-depot setup *init-depot :random)
  (declare-depot setup *term-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition
    ;; on task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *init-depot
      ;;
      (source> *init-depot)

      ;; {"a" 1, "b" 2}
      (local-transform> [(keypath "a") (termval 1)] $$p)
      (local-transform> [(keypath "b") (termval 2)] $$p)

      ;; *term-depot
      ;;
      (source> *term-depot)

      (<<ramafn %plus10 [*curr]
        (:> (+ 10 *curr)))
      (local-transform> [(keypath "a") (term %plus10)] $$p)

      ;; (putval val)
      ;;
      ;; Adds an external value to the collected vals. Useful when
      ;; additional arguments are required to the transform function
      ;; that would otherwise require partial application or a wrapper
      ;; function.
      ;;
      ;; NB: we could instead have used (term (partial %f 10 20)) to the
      ;; same effect.

      (<<ramafn %f [*arg1 *arg2 *v]
        (println "term function args:" (str *v ", " *arg1 ", " *arg2))
        (:> (+ *arg2 (* *v *arg1))))
      (local-transform> [(keypath "b") (putval 10) (putval 20) (term %f)] $$p))))

;; EXPECTED OUTPUT
;;
;; Initial value: {a 1, b 2}
;; term function args:  2, 10, 20
;; After transform: {a 11, b 40}

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc MultiArityTermModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name MultiArityTermModule)
          ;; depots
          init-depot (foreign-depot ipc module-name "*init-depot")
          term-depot (foreign-depot ipc module-name "*term-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! init-depot nil)
      (println "Initial value:" (foreign-select-one STAY pstate))

      (foreign-append! term-depot nil)
      (println "After transform:" (foreign-select-one STAY pstate)))))
