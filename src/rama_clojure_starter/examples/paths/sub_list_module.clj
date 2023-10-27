(ns rama-clojure-starter.examples.paths.sub-list-module
  "Port of rama.examples.paths.SubListModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

(defmodule SubListModule [setup topologies]
  ;; Depots
  (declare-depot setup *init-depot :random)
  (declare-depot setup *sub-list-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition
    ;; on task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *init-depot
      ;;
      (source> *init-depot)

      ;; {"k" ["a" "b" "c" "d" "e" "f"]}
      (local-transform> [(keypath "k") AFTER-ELEM (termval "a")] $$p)
      (local-transform> [(keypath "k") AFTER-ELEM (termval "b")] $$p)
      (local-transform> [(keypath "k") AFTER-ELEM (termval "c")] $$p)
      (local-transform> [(keypath "k") AFTER-ELEM (termval "d")] $$p)
      (local-transform> [(keypath "k") AFTER-ELEM (termval "e")] $$p)
      (local-transform> [(keypath "k") AFTER-ELEM (termval "f")] $$p)

      ;; *sub-list-depot
      ;;
      (source> *sub-list-depot)

      (<<ramafn %f [*arr]
        (:> (reverse *arr)))
      (local-transform> [(keypath "k") (srange 1 5) (term %f)] $$p))))

;; EXPECTED OUTPUT
;;
;; Initial nested list: (a b c d e f)
;; After transform: (a e d c b f)

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc SubListModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name SubListModule)
          ;; depots
          init-depot (foreign-depot ipc module-name "*init-depot")
          sub-list-depot (foreign-depot ipc module-name "*sub-list-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! init-depot nil)
      (println "Initial nested list:" (foreign-select-one [(keypath "k")] pstate))

      (foreign-append! sub-list-depot nil)
      (println "After transform:" (foreign-select-one [(keypath "k")] pstate)))))
