(ns rama-clojure-starter.my-module
  (:require
   [clojure.string :as cstr])
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]))

(defmodule MyModule [setup topologies])
