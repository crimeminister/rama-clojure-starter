(ns rama-clojure-starter.examples.paths.map-key-module
  "Port of rama.examples.paths.MapKeyModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

(defmodule MapKeyModule [setup topologies]
  ;; Depots
  (declare-depot setup *init-depot :random)
  (declare-depot setup *map-key-depot :random)

  ;; Topology: s
  (let [s (stream-topology topologies "s")]
    ;; The global? option causes the PState to have a single partition
    ;; on task 0 rather than a partition on every task.
    (declare-pstate s $$p Object {:global? true})

    (<<sources s
      ;; *init-depot
      ;;
      (source> *init-depot)

      (local-transform> [(keypath "a") (termval 0)] $$p)
      (local-transform> [(keypath "b") (termval 1)] $$p)

      ;; *map-key-depot
      ;;
      (source> *map-key-depot)

      ;; (map-key key)
      ;;
      ;; https://github.com/redplanetlabs/specter/wiki/List-of-Navigators#map-key
      ;;
      ;; Navigates to the given key in the map (not to the value).

      ;; Rename map key "a" to "c":
      ;; {"a" 0, "b" 1} => {"b" 1, "c" 0}
      (local-transform> [(map-key "a") (termval "c")] $$p))))

;; EXPECTED OUTPUT
;;
;; Init: [[a 0] [b 1]]
;; After transform: [[b 1] [c 0]]

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc MapKeyModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name MapKeyModule)
          ;; depots
          init-depot (foreign-depot ipc module-name "*init-depot")
          map-key-depot (foreign-depot ipc module-name "*map-key-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$p")]

      (foreign-append! init-depot nil)
      (println "Init:" (foreign-select ALL pstate))

      (foreign-append! map-key-depot nil)
      (println "After transform:" (foreign-select ALL pstate)))))
