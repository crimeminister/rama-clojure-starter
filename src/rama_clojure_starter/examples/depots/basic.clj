(ns rama-clojure-starter.examples.depots.basic
  "Port of rama.examples.depots.BasicDepotExamplesModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.test :as rtest]))

(defmodule BasicDepotExamplesModule [setup topologies]
  ;; Depots

  ;; Each append goes to a random partition.
  (declare-depot setup *depot1 :random)
  ;; Each append goes to a partition determined by the hash of the value
  ;; extracted by the provided function. The provided function may be a
  ;; keyword or a top-level Clojure function.
  (declare-depot setup *depot2 (hash-by first))
  ;; Appends from clients are not allowed, and attempting one will
  ;; result in an exception on the client.
  (declare-depot setup *depot3 :disallow))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc BasicDepotExamplesModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name BasicDepotExamplesModule)
          ;; depots
          depot1 (foreign-depot ipc module-name "*depot1")
          depot2 (foreign-depot ipc module-name "*depot2")
          depot3 (foreign-depot ipc module-name "*depot3")]

      (println "created three depots"))))
