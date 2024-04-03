(ns rama-clojure-starter.examples.query.simple
  "Port of rama.examples.query.SimpleQueryModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest]))


(defmodule SimpleQueryModule [setup topologies]
  (<<query-topology
   topologies "q"
   [*a *b :> *res]
   (+ *a *b 1 :> *res)
   (|origin)))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc SimpleQueryModule {:tasks 4 :threads 4})
    (let [module-name (get-module-name SimpleQueryModule)
          ;; query
          q (foreign-query ipc module-name "q")]

      (println "Query 1:" (foreign-invoke-query q 1 2))
      (println "Query 2:" (foreign-invoke-query q 10 7)))))
