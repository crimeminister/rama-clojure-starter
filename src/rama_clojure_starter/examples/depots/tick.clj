(ns rama-clojure-starter.examples.depots.tick
  "Port of rama.examples.depots.TickDepotModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.test :as rtest]))


(defmodule TickDepotModule [setup topologies]
  ;; Depots

  ;; Declare a tick depot that emits every 3000 milliseconds. Can be
  ;; used in any ETL topology and cannot be appended to by clients.
  (declare-tick-depot setup *ticks 3000)

  (let [s (stream-topology topologies "s")]
    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the tick depot.
     (source> *ticks)
     ;; Print a message to the console for each tick that is emitted.
     (println "Tick"))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc TickDepotModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name TickDepotModule)]
      ;; Let the topology run for this long before terminating.
      (Thread/sleep 10000))))
