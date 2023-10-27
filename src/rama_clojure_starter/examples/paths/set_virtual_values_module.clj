(ns rama-clojure-starter.examples.paths.set-virtual-values-module
  "Port of rama.examples.paths.SetVirtualValuesModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

(defmodule SetVirtualValuesModule [setup topologies]
  ;; Depots
  (declare-depot setup *depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition
    ;; on task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *depot
      ;;
      (source> *depot :> *v)

      ;; NONE-ELEM navigates to the 'void' element in a set. For
      ;; transformations, if the result is not NONE, then add that value
      ;; to the set.
      ;;
      ;; cf. https://github.com/redplanetlabs/specter/wiki/List-of-Navigators#none-elem

      (local-transform> [NONE-ELEM (termval *v)] $$p))))

;; EXPECTED OUTPUT
;;
;; After first transform: [a]
;; After second transform: [f a]
;; After third transform: [f a c]

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc SetVirtualValuesModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name SetVirtualValuesModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! depot "a")
      (println "After first transform:" (foreign-select ALL pstate))

      (foreign-append! depot "f")
      (println "After second transform:" (foreign-select ALL pstate))

      (foreign-append! depot "c")
      (println "After third transform:" (foreign-select ALL pstate)))))
