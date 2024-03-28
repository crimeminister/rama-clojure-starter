(ns rama-clojure-starter.examples.pstates.multi
  "Port of rama.examples.pstates.MultiSubscribeModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule MultiSubscriberModule [setup topologies]
  ;; Depots
  (declare-depot setup *depot (hash-by identity))

  (let [s (stream-topology topologies "s")]
    (declare-pstate s $$p {String Long} {:global? true})

    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *depot :> *k)
     ;; Store a count of the number of times the value has been seen.
     (local-transform> [(keypath *k) (nil->val 0) (term inc)] $$p))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc MultiSubscriberModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name MultiSubscriberModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")
          ;; pstates
          p (foreign-pstate ipc module-name "$$p")]

      (foreign-append! depot "a")
      (foreign-append! depot "b")
      (foreign-append! depot "c")

      (let [callback-top (fn [new-val diff old-val]
                           (println "'top' callback:" new-val diff old-val))
            proxy-top (foreign-proxy-async (keypath STAY) p {:callback-fn callback-top})

            callback-a (fn [new-val diff old-val]
                         (println "'a' callback:" new-val diff old-val))
            proxy-a (foreign-proxy-async (keypath "a") p {:callback-fn callback-a})

            callback-b (fn [new-val diff old-val]
                         (println "'b' callback:" new-val diff old-val))
            proxy-b (foreign-proxy-async (keypath "b") p {:callback-fn callback-b})]

        (foreign-append! depot "a")
        (foreign-append! depot "a")
        (foreign-append! depot "c")
        (foreign-append! depot "b")

        (Thread/sleep 50)))))
