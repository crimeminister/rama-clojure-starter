(ns rama-clojure-starter.examples.depots.profile-good
  "Port of rama.examples.depots.ProfileFieldSetGoodModule Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.path :refer :all]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.test :as rtest]))


(defmodule ProfileFieldSetGoodModule [setup topologies]
  ;; Depots
  (declare-depot setup *profile-fields-depot (hash-by first))

  (let [s (stream-topology topologies "profiles")]
    ;; Store user profile fields as triples: {$user-id {$field
    ;; $value}}. Note that the Java example uses String as the schema
    ;; type for the field name, but we prefer Keyword here as that seems
    ;; more idiomatic in Clojure.
    (declare-pstate s $$profiles {String {clojure.lang.Keyword Object}})

    ;; Define the ETL logic using Rama's dataflow API.
    (<<sources
     s
     ;; Subscribe the topology to the incoming depot.
     (source> *profile-fields-depot :> *tuple)
     ;; Emits every element of input list as a separate field. Useful
     ;; for processing elements of fixed-size lists.
     (ops/expand *tuple :> *user-id *field *value)
     ;; Write the provided profile field data into the pstate using
     ;; local-transform>, which transforms the PState partition on current
     ;; task with the given path. Transform path must use term, termval,
     ;; or NONE> at leaves.
     (local-transform> [(keypath *user-id *field) (termval *value)]
                       $$profiles))))


(defn -main []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc ProfileFieldSetGoodModule {:tasks 1 :threads 1})
    (let [module-name (get-module-name ProfileFieldSetGoodModule)
          ;; depots
          profile-fields-depot (foreign-depot ipc module-name "*profile-fields-depot")
          ;; pstates
          pstate (foreign-pstate ipc module-name "$$profiles")]

      ;; Add a couple of users and related profile data.
      (foreign-append! profile-fields-depot ["foobar" :first-name "Foo"])
      (foreign-append! profile-fields-depot ["foobar" :last-name "Bar"])

      (foreign-append! profile-fields-depot ["bizbaz" :first-name "Biz"])
      (foreign-append! profile-fields-depot ["bizbaz" :last-name "Baz"])

      (println "user profiles:" (foreign-select ALL pstate)))))
