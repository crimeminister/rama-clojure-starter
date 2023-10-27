(ns rama-clojure-starter.examples.paths.subselect-module
  "Port of rama.examples.paths.SubSelectModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule SubselectModule [setup topologies]
  ;; Depots
  (declare-depot setup *init-depot :random)
  (declare-depot setup *subselect-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition on
    ;; task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *init-depot
      ;;
      (source> *init-depot)

      ;; https://github.com/redplanetlabs/specter/wiki/List-of-Navigators#after-elem
      ;;
      ;; AFTER-ELEM navigates to the 'void' element after the
      ;; sequence. For transformations, if result is not NONE, then
      ;; append that value.

      ;; https://github.com/redplanetlabs/specter/wiki/List-of-Navigators#last
      ;;
      ;; LAST navigates to the last element of a collection. If the
      ;; collection is a map, returns a key-value pair [key value]. If
      ;; the collection is empty, navigation stops.

      ;; Update $$p to contain:
      ;; ((1 2 3 4) (5 6 7) (8 9 10))

      (local-transform> [AFTER-ELEM (termval nil)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 1)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 2)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 3)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 4)] $$p)

      (local-transform> [AFTER-ELEM (termval nil)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 5)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 6)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 7)] $$p)

      (local-transform> [AFTER-ELEM (termval nil)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 8)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 9)] $$p)
      (local-transform> [LAST AFTER-ELEM (termval 10)] $$p)

      ;; *subselect-depot
      ;;
      (source> *subselect-depot)

      ;; Defines an anonymous operation that is used in the
      ;; local-transform to reverse a collection of values.
      (<<ramafn %reverse [*curr]
        (:> (reverse *curr)))

      ;; (subselect & path)
      ;;
      ;; https://redplanetlabs.com/clojuredoc/com.rpl.rama.path.html#var-subselect
      ;;
      ;; Navigates to list of values navigated by provided
      ;; path. Transforms on that list will update original locations of
      ;; each value, no matter how nested. Control navigator.

      (local-transform> [(subselect [ALL ALL odd?])
                         (term %reverse)]
                        $$p))))

;; EXPECTED OUTPUT
;;
;; Initial contents: ((1 2 3 4) (5 6 7) (8 9 10))
;; After transform: ((9 2 7 4) (5 6 3) (8 1 10))

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc SubselectModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name SubselectModule)
          ;; depots
          init-depot (foreign-depot ipc module-name "*init-depot")
          subselect-depot (foreign-depot ipc module-name "*subselect-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! init-depot nil)
      (println "Initial contents:" (foreign-select STAY pstate))

      (foreign-append! subselect-depot nil)
      (println "After transform:" (foreign-select STAY pstate)))))
