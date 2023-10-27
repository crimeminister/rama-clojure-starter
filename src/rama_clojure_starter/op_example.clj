(ns rama-clojure-starter.op-example
  (:require
   [com.rpl.rama :refer :all]
   [com.rpl.rama.ops :as ops]))

;; -----------------------------------------------------------------------------
;; Custom Operations

;; Define a custom operation that emits multiple times. Note that
;; when :> is used as an operation it emits to the :> output of the
;; caller (also known as "invoking the continuation").
(deframaop foo [*a]
  (:> (inc *a))
  (:> (dec *a)))

;; The ?<- macro compiles and executes a block of dataflow code.
(?<-
  ;; When foo is called, the following code is executed for each output
  ;; that it emits.
  (foo 5 :> *v)
  (println *v))

;; -----------------------------------------------------------------------------
;; Filtering

;; Only emits when the value is true; this operates equivalently to
;; the built-in operation (filter>).
(deframaop my-filter [*v]
  (<<if *v
        (:>)))

(?<-
  ;; range> is like Clojure's range except that it emits per value
  ;; rather than returning a sequence.
  (ops/range> 0 5 :> *v)
  (my-filter (even? *v))
  (println *v))

;; -----------------------------------------------------------------------------
;; Conditionals

;; <<if is the most common way to write dataflow conditionals:
(?<-
  (<<if (= 1 2)
   (println "true branch 1")
   (println "true branch 2")
   (else>)
   (println "else branch 1")
   (println "else branch 2")))

;; <<if is built upon the more primtive if>, which is an operation that
;; emits to the :then> and :else> output streams. Using these
;; primitives, the previous code can be expressed like this:

(?<-
  (if> (= 1 2) :then> <then> :else>)
  (println "else branch 1")
  (println "else branch 2")
  (hook> <then>)
  (println "true branch 1")
  (println "true branch 2"))

;; Note that operations can emit to other output streams besides :>, in
;; this case to :then> and :else>.

;; Symbols surrounded with < and > are called "anchors", and they label
;; a point in dataflow code. By default, dataflow code attaches to the
;; previousu code, but if you use hook> then you can change where
;; subsequent code attaches.

;; if> is not a special form, it can be possed around.

(deframaop bar [%f *v]
  ;; %f is an anonymous operation, *v is a value.
  ;; There is an implicit, empty :else> branch.
  (%f *v :then>)
  (:>))

;; "B" does not print since %f emits to the :else> branch, which has no
;; code attached to it.
(?<-
  (bar if> true)
  (println "A")
  (bar if> false)
  (println "B"))

;; -----------------------------------------------------------------------------
;; Functions

;; The general term for an operation in Rama is "fragment". This can be
;; a ramaop or ramafn.

;; A ramafn is an operation that emits exactly one time to :>, and
;; that's the last thing they do (like Clojure functions).

(deframafn myfn [*a *b]
  (:> (+ *a *b 10)))

;; A ramafn can be invoked from regular Clojure code as well as from
;; Rama code.
(myfn 1 2)

;; If a ramafn definition doesn't emit or emits multiple times, you'll
;; get a runtime error. A ramafn executes more efficiently than a ramaop
;; when the compiler knows a callsite is invoking a ramafn than a
;; ramaop.

;; -----------------------------------------------------------------------------
;; Loops

(?<-
  (loop<- [*v 0 :> *i]
    (println "loop iter")
    (<<if (< *v 5)
          (:> *v)
          (continue> (inc *v))))
  (println "emitted: " *i))
