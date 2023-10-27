(ns rama-clojure-starter.examples.auction
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.test :as rtest])
  (:import
   [com.rpl.rama.helpers ModuleUniqueIdPState TopologyScheduler]))

;; This code is taken from this blog post introducing Rama's Clojure API:
;; https://blog.redplanetlabs.com/2023/10/11/introducing-ramas-clojure-api/

;; Types
;; -----------------------------------------------------------------------------

(def _user-id Long)

(def _listing-id Long)

(def _post String)

;; Records
;; -----------------------------------------------------------------------------

;; The data type used to represent an auction listing.
(defrecord Listing [user-id post expiration-time-millis])

;; Used by the topology that handles notifications.
(defrecord ListingWithId [id listing])

;; A bid of a listing.
(defrecord Bid [bidder-id user-id listing-id amount])

;; Listings have their own ID, but their PStates are partitioned by the
;; user ID that made the listing. This keeps the bidder / "top bid"
;; information for a listing colocated with its information in the
;; $$user-listings PState (generally a good idea as it speeds up queries
;; that want to look at multiple PStates at the same time). With this
;; design, to look up information about a Listing you need both the
;; listing ID and the owning user ID (to find the right PState
;; partition). This is why the $$user-bids PState tracks a
;; ListingPointer rather than just a listing ID.
(defrecord ListingPointer [user-id listing-id])

;; Helpers
;; -----------------------------------------------------------------------------

(defn owner-notification [listing-id winner-id amount]
  (if winner-id
    (str "Auction for listing " listing-id
         " finished with winner " winner-id
         " for the amount " amount)
    (str "Auction for listing " listing-id " finished with no winner")))

(defn winner-notification [user-id listing-id amount]
  (str "You won the auction for listing " user-id "/" listing-id
       " for the amount " amount))

(defn loser-notification [user-id listing-id]
  (str "You lost the auction for listing " user-id "/" listing-id))

(defn sorted-set-last [^java.util.SortedSet set]
  (.last set))

;; Module
;; -----------------------------------------------------------------------------

(defmodule AuctionModule [setup topologies]
  ;; The depot that receives new listings. The partitioner hash-by
  ;; controls on which partition processing beings in the ETL. Since the
  ;; PState for listings will be partitioned by user IDs, setting the
  ;; depot partitioner this way means no further partitioning is needed
  ;; in the ETL logic. This is simpler and more efficient than using
  ;; a :random depot partitioner as in the word count example.
  (declare-depot setup *listing-depot (hash-by :user-id))
  ;; Stores Bid objects.
  (declare-depot setup *bid-depot (hash-by :user-id))
  ;; When a listing is assigned an ID, a ListingWithId object will be
  ;; added to this depot. This is used by a separate topology that
  ;; generates notifications. The :disallow partitioner disallows
  ;; records to be appended to this depot using
  ;; foreign-append!. Instead, the module appends to this depot as it
  ;; generates listing IDs.
  (declare-depot setup *listing-with-id-depot :disallow)

  ;; topology: auction
  ;;
  ;; This topology processes incoming auction listings.
  (let [s (stream-topology topologies "auction")
        ;; This utility generates unique 64-bit IDs. It works by
        ;; declaring a PState tracking a counter on each task and
        ;; combining that counter with the task ID when generating an
        ;; ID.
        idgen (ModuleUniqueIdPState. "$$id")]
    ;; This PState stores every listing made by a user in a submap,
    ;; which is subindexed; its elements are indexed individually to
    ;; allow the submap to be read and written to efficiently even when
    ;; it grows larger than memory. This is appropriate since a user can
    ;; have an arbitrary number of listings.
    ;;
    ;; schema: {user-id {listing-id post}}
    (declare-pstate s $$user-listings {Long (map-schema Long String {:subindex? true})})
    ;; Store the set of bidders for a given listing.
    ;;
    ;; schema: {listing-id #{user-id}}
    (declare-pstate s $$listing-bidders {Long (set-schema Long {:subindex? true})})
    ;; Store the top bid for a given listing.
    ;;
    ;; schema: {listing-id {:user-id user-id :amount Long}}
    (declare-pstate s $$listing-top-bid {Long (fixed-keys-schema {:user-id Long :amount Long})})
    ;; Store the user bids on a listing.
    ;;
    ;; schema: {listing-id {[user-id listing-id] amount}}
    (declare-pstate s $$user-bids {Long (map-schema ListingPointer Long {:subindex? true})})

    ;; Link the ID generator to the PState that it uses to store a
    ;; counter (in topology s).
    (.declarePState idgen s)

    ;; Define the ETL that maintains the $$user-listings PState.
    ;;
    ;; Note that only one event runs on a module task at a time. While
    ;; this logic is executing, nothing else is running on this task:
    ;; other bid events, foreign PState queries, or other events. Thus,
    ;; it's impossible for multiple bids to update the $$listing-top-bid
    ;; PState at the same time. The colocation of computation and
    ;; storage in Rama gives you the atomicity and transactional
    ;; properties needed for this use case.
    (<<sources s
      (source> *listing-depot :> {:keys [*user-id *post] :as *listing})
      ;; java-macro! allows dataflow code generated by the Java API to
      ;; be used directly in the Clojure API. In this case, .genId binds
      ;; a new variable *listing-id with a newly generated ID.
      (java-macro! (.genId idgen "*listing-id"))
      ;; Write the listing into the PState under the correct keys.
      (local-transform> [(keypath *user-id *listing-id) (termval *post)]
                        $$user-listings)
      ;; Write the listing ID and listing to a depot. Unlike
      ;; foreign-append!, depot appends within topologies go directly to
      ;; the depot partition colocated with the ETL event, which is
      ;; consistent with how writes to PStates work. Because there's no
      ;; other partitioning here, *listing-with-id-depot will be
      ;; partitioned exactly the same as *listing-depot.
      (depot-partition-append! *listing-with-id-depot
                               (->ListingWithId *listing-id *listing)
                               :append-ack)

      (source> *bid-depot :> {:keys [*bidder-id *user-id *listing-id *amount]})
      ;; Retrieve the listing and check if it already over. The
      ;; $$finished-listings PState is a map from listing to flag. If
      ;; the auction is over, the bid is ignored.
      (local-select> (keypath *listing-id) $$finished-listings :> *finished?)
      (filter> (not *finished?))

      ;; Store the bidder ID in the list of bidders for an auction.
      (local-transform> [(keypath *listing-id) NONE-ELEM (termval *bidder-id)]
                        $$listing-bidders)
      ;; Update the top bid amount if the current bid has a larger value.
      (local-transform> [(keypath *listing-id)
                         (selected? :amount (nil->val 0) (pred< *amount))
                         (termval {:user-id *bidder-id :amount *amount})]
                        $$listing-top-bid)
      ;; Store the user's bid in the $$user-bids PState. This PState is
      ;; partitioned by the bidder's user ID, so a |hash partition is
      ;; done first to relocate the computation to the correct task.
      (|hash *bidder-id)
      (->ListingPointer *user-id *listing-id :> *pointer)
      (local-transform> [(keypath *bidder-id *pointer) (termval *amount)]
                        $$user-bids)))

  ;; topology: expirations
  ;;
  ;; This microbatch topology processes a small batch of data across
  ;; all depot partitions at the same time. Each iteration processes
  ;; the data that accumulated since the last microbatch
  ;; iteration. This has higher throughput (because lower overhead
  ;; than streaming) but lower latency (a few hundred milliseconds vs
  ;; a few milliseconds). This also provides exactly-once processing
  ;; semantics for updates to PStates even in case of failure and
  ;; retries.
  ;;
  ;; Because the topology is in the same module, it is colocated with
  ;; the "auction" streaming topology and all its PStates. They share
  ;; resources and can read each other's PStates directly.
  (let [mb (microbatch-topology topologies "expirations")
        ;; A utility that makes it easy to schedule future work in a
        ;; topology. Built upon ETLs and PStates, it's completely
        ;; fault-tolerant and can sustain very high throughputs of
        ;; scheduled events.
        scheduler (TopologyScheduler. "$$scheduler")]
    ;; Store whether or not a listing is already finished.
    ;;
    ;; schema: {listing-id finished?}
    (declare-pstate mb $$finished-listings {Long Boolean})
    ;; A list of notification strings for each user. Since
    ;; notifications are unbounded, the list is subindexed.
    ;;
    ;; schema: {user-id message}
    (declare-pstate mb $$notifications {Long (vector-schema String {:subindex? true})})

    ;; Attach the schedule to the topology.
    (.declarePStates scheduler mb)

    ;; ETL
    (<<sources mb
      ;; %microbatch is the entire batch of data for this
      ;; %iteration. It's an anonymous operation which when invoked
      ;; %emits all data across all depot partitions.
      (source> *listing-with-id-depot :> %microbatch)
      (anchor> <root>)
      (%microbatch :> {*listing-id :id
                       {:keys [*user-id *expiration-time-millis]} :listing})
      ;; Schedule a tuple containing user ID and listing ID for later
      ;; execution at the specified time.
      (vector *user-id *listing-id :> *tuple)
      (java-macro! (.scheduleItem scheduler "*expiration-time-millis" "*tuple"))

      (hook> <root>)
      (java-macro!
       ;; .handleExpirations on TopologyScheduler inserts code that
       ;; checks for expired items. Here it's attached to the root of
       ;; the microbatch iteration so it will run once for each
       ;; microbatch.
       (.handleExpirations
        scheduler
        ;; Each expired item is bound to *tuple.
        "*tuple"
        ;; The time at which it checked is bound to *current-time-millis.
        "*current-time-millis"
        ;; The provided block of code is executed; the java-block<-
        ;; macro defines a block of code for the Java API in Clojure.
        (java-block<-
          ;; Each tuple represents an auction that has completed; the
          ;; owning user ID and listing ID are provided.
          (identity *tuple :> [*user-id *listing-id])
          ;; Set a flag in $$finished-listings to indicate that the
          ;; auction is over. Note that while this event is running no
          ;; other events can run on this partition, so there are no
          ;; race conditions with concurrent bids.
          (local-transform> [(keypath *listing-id) (termval true)]
                            $$finished-listings)
          ;; Emit the winner ID along with their winning bid amount.
          (local-select> (keypath *listing-id)
                         $$listing-top-bid
                         :> {*winner-id :user-id *amount :amount})
          ;; Store an owner notification. Since listings are partitioned
          ;; by their owner's user ID, no partitioning is needed to
          ;; deliver this partition.
          (local-transform> [(keypath *user-id)
                             AFTER-ELEM
                             (termval (owner-notification *listing-id *winner-id *amount))]
                            $$notifications)
          ;; Since the number of bidders for a listing can be
          ;; arbitrarily large, this code paginates through the PState
          ;; in chunks.
          (loop<- [*next-id -1 :> *bidder-id]
            ;; Note that nothing else can run on a task thread while an
            ;; event is running. This allows us to atomically read and
            ;; write to many PStates at once, while also avoiding
            ;; potential race conditions. The caveat is that app
            ;; developers need to make sure not to hold a thread for too
            ;; long to avoid unfairly dlaying events for PState reads
            ;; and other ETLs. Rama apps require *cooperative
            ;; multitasking*. We call yield-if-overtime to yield the
            ;; thread to other events if too much time has passed (by
            ;; default 5 ms).
            (yield-if-overtime)
            ;; Fetch all bidders from $$listing-bidders.
            (local-select> [(keypath *listing-id)
                            (sorted-set-range-from *next-id {:max-amt 1000 :include? false})]
                           $$listing-bidders
                           :> *users)
            ;; Each bidder is emitted from the loop separately.
            (<<atomic
             (:> (ops/explode *users)))
            ;; Handle chunks of 1,000 bidders per loop iteration.
            (<<if (= (count *users) 1000)
                  (continue> (sorted-set-last *users))))

          ;; Switch to the task hosting notifications for a bidder.
          (|hash *bidder-id)
          ;; Update the $$notifications PState with the appropriate
          ;; notification for each bidder.
          (<<if (= *bidder-id *winner-id)
                (winner-notification *user-id *listing-id *amount :> *text)
                (else>)
                (loser-notification *user-id *listing-id :> *text))
          (local-transform> [(keypath *bidder-id)
                             AFTER-ELEM
                             (termval *text)]
                            $$notifications)
          ))))))


(comment
  ;; Add bids to a listing.
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc AuctionModule {:tasks 4 :threads 2})
    (let [module-name (get-module-name AuctionModule)
          listing-depot (foreign-depot ipc module-name "*listing-depot")
          bid-depot (foreign-depot ipc module-name "*bid-depot")
          user-listings (foreign-pstate ipc module-name "$$user-listings")
          listing-bidders (foreign-pstate ipc module-name "$$listing-bidders")
          listing-top-bid (foreign-pstate ipc module-name "$$listing-top-bid")
          user-bids (foreign-pstate ipc module-name "$$user-bids")

          larry-id 0
          hank-id 1
          artie-id 2
          beverly-id 3

          _ (foreign-append! listing-depot (->Listing larry-id "Listing 1" 0))
          larry1 (foreign-select-one [(keypath larry-id) LAST FIRST] user-listings)]
      ;; Add bids on a listing: [-id user-id listing-id amount]
      (foreign-append! bid-depot (->Bid hank-id larry-id larry1 45))
      (foreign-append! bid-depot (->Bid artie-id larry-id larry1 50))
      (foreign-append! bid-depot (->Bid beverly-id larry-id larry1 48))

      ;; ...and print what was added to the $$user-listings PState.
      (println "Listing bidders:" (foreign-select [(keypath larry1) ALL]
                                                  listing-bidders
                                                  ;; determine which partition to query
                                                  {:pkey larry-id}))
      (println "Top bid:" (foreign-select-one (keypath larry1)
                                              listing-top-bid
                                              {:pkey larry-id}))
      (println "Hank's bids:" (foreign-select [(keypath hank-id) ALL] user-bids))))
  )

(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc AuctionModule {:tasks 4 :threads 2})
    (let [expiration (fn [seconds]
                       (+ (System/currentTimeMillis) (* seconds 1000)))
          module-name (get-module-name AuctionModule)
          ;; depots
          listing-depot (foreign-depot ipc module-name "*listing-depot")
          bid-depot (foreign-depot ipc module-name "*bid-depot")
          ;; pstates
          user-bids (foreign-pstate ipc module-name "$$user-bids")
          user-listings (foreign-pstate ipc module-name "$$user-listings")
          listing-bidders (foreign-pstate ipc module-name "$$listing-bidders")
          listing-top-bid (foreign-pstate ipc module-name "$$listing-top-bid")
          notifications (foreign-pstate ipc module-name "$$notifications")

          larry-id 0
          hank-id 1
          artie-id 2
          beverly-id 3

          _ (foreign-append! listing-depot (->Listing larry-id "Listing 1" (expiration 5)))

          larry1 (foreign-select-one [(keypath larry-id) LAST FIRST] user-listings)]
      (foreign-append! bid-depot (->Bid hank-id larry-id larry1 45))
      (foreign-append! bid-depot (->Bid artie-id larry-id larry1 50))
      (foreign-append! bid-depot (->Bid beverly-id larry-id larry1 48))

      ;; Wait slightly more than the expiration time for the listing to
      ;; allow notifications to be delivered.
      (Thread/sleep 6000)

      (println "Larry:" (foreign-select [(keypath larry-id) ALL] notifications))
      (println "Hank:" (foreign-select [(keypath hank-id) ALL] notifications))
      (println "Artie:" (foreign-select [(keypath artie-id) ALL] notifications))
      (println "Beverly:" (foreign-select [(keypath beverly-id) ALL] notifications)))))
