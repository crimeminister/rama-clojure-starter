(ns rama-clojure-starter.examples.query.reach
  "Port of rama.examples.query.ReachModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.test :as rtest]))

;; Implements part of the ETL for determining partial reach counts. The
;; reach count is the transitive number of user IDs that follow a
;; particular URL.
;;
;; A "generator" processes a batch of data and emits a new batch of data
;; after performing any amount of computation, aggregation, joins, or
;; other logic on it. defgenerator is a Clojure function that returns a
;; subbatch defined with batch<- for use in a <<batch or
;; <<query-topology.
(defgenerator partial-reach-counts
  [*url]
  (batch<- [*count]
           ;; Move to the partition where the *url is kept.
           (|hash *url)
           ;; Emit the user IDs of users that follow the given URL. This
           ;; is done using local-select> which queries the PState
           ;; partition on the current task using provided path.
           (local-select> [(keypath *url) ALL] $$url-to-users :> *user-id)
           ;; Queries the PState using the given path, changing
           ;; partitions based on the path. Emits each follower user ID.
           (select> [(keypath *user-id) ALL] $$followers :> *reached-user-id)
           ;; Chooses task to partition to based on hash(field) %
           ;; num-tasks.
           (|hash *reached-user-id)
           ;; Aggregate a set of follower user IDs. This is to eliminate
           ;; double-counting in the results by collecting only unique
           ;; follower IDs in spite of any repetition in the depot data.
           (aggs/+set-agg *reached-user-id :> *partial-reached-set)
           ;; Emit the size of the partial unique follower ID set.
           (count *partial-reached-set :> *count)))

(defmodule ReachModule [setup topologies]
  ;; Depots
  (declare-depot setup *urls-depot (hash-by first))
  (declare-depot setup *follows-depot (hash-by first))

  (let [core (microbatch-topology topologies "core")]
    ;; PStates
    ;;

    ;; A map from URL to username.
    (declare-pstate core $$url-to-users {String (set-schema String {:subindex? true})})
    ;; A map from user ID (followed) to user ID (follower).
    (declare-pstate core $$followers {String (set-schema String {:subindex? true})})

    ;; Flows
    ;;

    ;; The *urls-depot contains pairs of [URL user-id]. Convert this
    ;; data into the PState map {*url #{...user-id...}}.
    (<<sources
     core
     (source> *urls-depot :> %microbatch)
     (%microbatch :> [*url *user-id])
     (+compound $$url-to-users {*url (aggs/+set-agg *user-id)}))

    ;; The *follows-depot contains pairs of [user-id
    ;; follower-id]. Convert this data into the PState map {*user-id
    ;; #{...*follower-id...}}.
    (<<sources
     core
     (source> *follows-depot :> %microbatch)
     (%microbatch :> [*user-id *follower-id])
     (+compound $$followers {*user-id (aggs/+set-agg *follower-id)})))

  ;; Queries
  ;;

  ;; NB: query topologies are implicitly batch blocks.
  (<<query-topology topologies "reach"
   [*url :> *num-unique-users]
   (partial-reach-counts *url :> *partial-count)
   ;; Every query topology must have an |origin call, which indicates to
   ;; move the computation back to where the query started.
   (|origin)
   ;; Sum the partial follower counts into the total number of unique
   ;; users that follow a URL.
   (aggs/+sum *partial-count :> *num-unique-users)))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc ReachModule {:tasks 2 :threads 2})
    (let [module-name (get-module-name ReachModule)
          ;; depot
          urls-depot (foreign-depot ipc module-name "*urls-depot")
          follows-depot (foreign-depot ipc module-name "*follows-depot")
          ;; query
          reach (foreign-query ipc module-name "reach")]

      (foreign-append! urls-depot ["grapefruit.com" "jamescagney"])
      (foreign-append! urls-depot ["grapefruit.com" "maeclarke"])

      ;; Set the user-ids that are followed by "jamescagney".
      (foreign-append! follows-depot ["jamescagney" "joanleslie"])
      (foreign-append! follows-depot ["jamescagney" "henryfonda"])
      (foreign-append! follows-depot ["jamescagney" "arlenefrancis"])
      (foreign-append! follows-depot ["jamescagney" "jacklemmon"])

      ;; Set the user-ids that are followed by "maeclarke".
      (foreign-append! follows-depot ["maeclarke" "henryfonda"])
      (foreign-append! follows-depot ["maeclarke" "charleslaughton"])
      (foreign-append! follows-depot ["maeclarke" "joanleslie"])
      (foreign-append! follows-depot ["maeclarke" "debbiereynolds"])

      ;; Wait until all 10 records have been processed as a microbatch.
      (rtest/wait-for-microbatch-processed-count ipc module-name "core" 10)

      ;; Expected value: 6
      (println "grapefruit.com reach:" (foreign-invoke-query reach "grapefruit.com")))))
