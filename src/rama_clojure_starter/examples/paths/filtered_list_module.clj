(ns rama-clojure-starter.examples.paths.filtered-list-module
  "Port of rama.examples.paths.FilteredListModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

(defmodule FilteredListModule [setup topologies]
  ;; Depots
  (declare-depot setup *init-depot :random)
  (declare-depot setup *filtered-list-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition on
    ;; task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *init-depot
      ;;
      (source> *init-depot)

      ;; Initialize $$p with [0 1 2 3 4 5].
      (local-transform> [AFTER-ELEM (termval 0)] $$p)
      (local-transform> [AFTER-ELEM (termval 1)] $$p)
      (local-transform> [AFTER-ELEM (termval 2)] $$p)
      (local-transform> [AFTER-ELEM (termval 3)] $$p)
      (local-transform> [AFTER-ELEM (termval 4)] $$p)
      (local-transform> [AFTER-ELEM (termval 5)] $$p)

      ;; *filtered-list-depot
      ;;
      (source> *filtered-list-depot)

      ;; Defines an anonymous operation that is used in the
      ;; local-transform to reverse a collection of values.
      (<<ramafn %reverse [*curr]
        (:> (reverse *curr)))

      (local-transform> [(filterer even?) (term %reverse)] $$p))))

;; EXPECTED OUTPUT
;;
;; Initial list: [0 1 2 3 4 5]
;; After transform: [4 1 2 3 0 5]

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc FilteredListModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name FilteredListModule)
          ;; depots
          init-depot (foreign-depot ipc module-name "*init-depot")
          filtered-list-depot (foreign-depot ipc module-name "*filtered-list-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! init-depot nil)
      (println "Initial list:" (foreign-select [ALL] pstate))

      (foreign-append! filtered-list-depot nil)
      (println "After transform:" (foreign-select [ALL] pstate)))))
