(ns rama-clojure-starter.examples.pstates.basic
  "Port of rama.examples.pstates.BasicPStateDeclarationsModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest]))


(defmodule BasicPStateDeclarationsModule [setup topologies]
  (let [s (stream-topology topologies "s")]
    (declare-pstate s $$p1 Long)
    (declare-pstate s $$p2 {String Integer})
    (declare-pstate s $$p3 {String (fixed-keys-schema {:count Integer
                                                       :some-list [String]
                                                       :some-set (set-schema Integer)})})))
