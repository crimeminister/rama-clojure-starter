(ns rama-clojure-starter.examples.depots.global
  "Port of rama.examples.depots.GlobalDepotModule Java example."
  (:require
   [com.rpl.rama :refer :all]))

(defmodule BasicDepotExamplesModule [setup topologies]
  ;; When :global? is true the depot should have only a single
  ;; partition. When this option is set, the partitioning scheme is
  ;; irrelevant.
  (declare-depot setup *depot1 :random {:global? true}))
