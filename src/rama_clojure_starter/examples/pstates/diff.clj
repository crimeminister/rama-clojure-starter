(ns rama-clojure-starter.examples.pstates.diff
  "Port of rama.examples.pstates.DiffProcessing Java example."
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.test :as rtest])
  (:import
   [com.rpl.rama.diffs KeysDiff NewValueDiff UnknownDiff Diff Diff$Processor KeyDiff KeyDiff$Processor]))

(defprotocol ProcessedKeys
  (pkeys [this] "Return the set of processed keys."))

(defn make-processor []
  (let [processed-keys (atom #{})]
    (reify
      ProcessedKeys
      (pkeys [this]
        @processed-keys)

      Diff$Processor
      (^void unhandled [this]
        (swap! processed-keys conj :unknown))

      KeyDiff$Processor
      (^void processKeyDiff [this ^KeyDiff diff]
       (swap! processed-keys conj (.getKey diff))))))

(defn process-keys-diff []
  (let [m {"a" (NewValueDiff. 1)
           "x" (NewValueDiff. 2)
           "d" (NewValueDiff. 3)
           "y" (NewValueDiff. 4)}
        ;; KeysDiff; changes for values for multiple keys in a data
        ;; structure, each with its own diff. Expands to multiple
        ;; KeyDiff.
        diff (KeysDiff. m)
        processor (make-processor)]
    (.process diff processor)
    (println "Processed keys 1:" (.pkeys processor)))

  (let [processor (make-processor)
        ;; UnknownDiff; the change between old and new values is
        ;; unknown.
        diff (UnknownDiff.)]
    (.process diff processor)
    (println "Processed keys 2:" (.pkeys processor))))

(defn -main []
  (process-keys-diff))
