(ns rama-clojure-starter.examples.word-count
  (:require
   [clojure.string :as str])
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

;; NB: symbols beginning with '*', '%', and '$$' are variables in Rama
;; dataflow code

;; - setup: used to declare depots and dependencies to depots, PStates,
;;   and query topologies in other modules
;; - topologies: used to declare ETL and query topologies
(defmodule WordCountModule [setup topologies]
  ;; Declare input depots where data originates. The depot is named
  ;; *sentences-depot (a dataflow variable useable later), and :random
  ;; is the partitioning scheme; in this case causing appended data to
  ;; go to a random depot partition.
  (declare-depot setup *sentences-depot :random)
  ;; Declare a *stream* ETL (data processing topology). Could also be a
  ;; microbatching topology instead.
  (let [s (stream-topology topologies "word-count")]
    ;; Declare an output PState (or index) called word-counts that maps
    ;; from String to Long.
    (declare-pstate s $$word-counts {String Long})
    ;; Define the data processing that is performed on data received by
    ;; the topology. The (<<sources) macro defines the ETL logic using
    ;; Rama's dataflow API, i.e. with "call-and-emit" code, where called
    ;; functions emit a value for processing by downstream code rather
    ;; than return a value to the caller.
    (<<sources s
      ;; Each call to (source>) subscribes a topology to a depot. This
      ;; subscribes to *sentences-depot and binds values from the depot
      ;; to the variable *sentence.
      (source> *sentences-depot :> *sentence)
      ;; Split incoming sentence strings on whitespace binding the
      ;; resulting sequence to the variable *words. The keyword :>
      ;; separates input from output.
      (str/split (str/lower-case *sentence) #" " :> *words)
      ;; The built-in (explode) operation emits each element of a
      ;; provided collection individually, binding it the variable
      ;; *word.
      (ops/explode *words :> *word)
      ;; A partitioner receives data, but may emit on a different thread
      ;; on a different machine. This call moves the computation
      ;; according to the hash of the value in *word. Thus, the same
      ;; word will always get processed by the same partition of the
      ;; module, while evenly distributing different words across
      ;; partitions.
      (|hash *word)
      ;; Update the word count for a word in the PState using "compound
      ;; aggregation", specifying the shape of the data to be
      ;; updated. Note that aggregators automatically take care of
      ;; initializing non-existent values, i.e. the word count starts at
      ;; 0 without you needing to specify so. By convention aggregator
      ;; names are prefixed with +.
      (+compound $$word-counts {*word (aggs/+count)}))))

(comment
  ;; To run using InProcessCluster from the REPL:
  (def ipc (rtest/create-ipc))
  (rtest/launch-module! ipc WordCountModule {:tasks 4 :threads 2})
  ;; The word "foreign" refers to Rama objects that live outside of modules.
  (def sentences-depot (foreign-depot ipc (get-module-name WordCountModule) "*sentences-depot"))
  (def word-counts (foreign-pstate ipc (get-module-name WordCountModule) "$$word-counts"))
  ;; Now append some data. By default, depot appends block until all
  ;; colocated stream topoligies have finished processing the
  ;; record. Note that these are blocking append and query methods;
  ;; corresponding async versions that return CompleteableFuture objects
  ;; are also available.
  (foreign-append! sentences-depot "Hello World")
  (foreign-append! sentences-depot "hello hello goodbye")
  (foreign-append! sentences-depot "Alice says hello")
  ;; Query the $$word-counts PState for the results.
  (foreign-select-one (keypath "hello") word-counts)
  (foreign-select-one (keypath "goodbye") word-counts)
  ;; Shut down the InProcessCluster.
  (close! ipc)
  )

;; The word "foreign" used below refers to Rama objects that live
;; outside of modules:
;; - foreign-depot: gets an external reference to a depot
;; - foreign-pstate: gets an external reference to a pstate

(defn -main []
  (with-open [;; Create an InProcessCluster.
              ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc WordCountModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name WordCountModule)
          ;; depots
          sentences-depot (foreign-depot ipc (get-module-name WordCountModule) "*sentences-depot")
          ;; pstates
          word-counts (foreign-pstate ipc (get-module-name WordCountModule) "$$word-counts")]
      ;; Now append some data. By default, depot appends block until all
      ;; colocated stream topoligies have finished processing the
      ;; record. Note that these are blocking append and query methods;
      ;; corresponding async versions that return CompleteableFuture objects
      ;; are also available.
      (foreign-append! sentences-depot "Hello World")
      (foreign-append! sentences-depot "hello hello goodbye")
      (foreign-append! sentences-depot "Alice says hello")
      ;; Query the $$word-counts PState for the results.
      (println "hello:" (foreign-select-one (keypath "hello") word-counts))
      (println "goodbye:" (foreign-select-one (keypath "goodbye") word-counts)))))
