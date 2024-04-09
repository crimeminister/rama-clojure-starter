(ns rama-clojure-starter.examples.integrating.async
  "Port of rama.examples.integrating.EachAsyncModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest]))


(defmodule EachAsyncModule [setup topologies]
  ;; Depots
  (declare-depot setup *depot :random)

  (let [s (stream-topology topologies "s")]
    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *depot)

     ;; <<ramaop defines an anonymous Rama operation within dataflow
     ;; code. Body can be asynchronous (if body is fully synchronous can
     ;; use <<ramafn). Output is done with :> segment.
     ;;
     ;; TODO is there are better way to translate this example?
     (<<ramaop %op []
      (:> "ABCDE"))
     (%op :> *v)

     (println "Result:" *v))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc EachAsyncModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name EachAsyncModule)
          ;; depots
          depot (foreign-depot ipc module-name "*depot")]

      (foreign-append! depot nil))))
