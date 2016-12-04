(ns dataframe.series
  (:refer-clojure)
  (:require [dataframe.util :refer :all]
            [clojure.string :as str])
  (:import (clojure.lang IPersistentVector IPersistentMap MapEntry)))

(declare series
         update-key)

; A Series is a data structure that maps an index
; A Series is a data structure that maps an index
; to valus.  It supports:
; - Order 1 access to values by index
; - Order 1 access to [index value] pairs by position (nth)
; - Maintaining the order of [index value] pairs for iteration
;
; As viewed as a Clojure Persistent collection, it is a collection
; of [index value] pairs.
; It is also Associative between the index keys and its values
(deftype ^{:protected true} Series [^IPersistentVector values
                                    ^IPersistentVector index
                                    ^IPersistentMap lookup]

  java.lang.Object
  (equals [this other]
    (cond (nil? other) false
          (not (= Series (class other))) false
          :else (every? true?
                        [(= (. this values) (. other values))
                         (= (. this index) (. other index))])))
  (hashCode [this]
    (hash [(hash (. this index)) (hash (. this values))]))

  java.lang.Iterable
  (iterator [this]
    (.iterator (zip index values)))

  clojure.lang.Counted
  (count [this] (count index))

  clojure.lang.IPersistentCollection
  (seq [this] (if (empty? index)
                nil
                (zip (. this index) (. this values))))
  (cons [this other]
    "Return a sequence of key-val pairs"
    (assert (vector? other))
    (assert 2 (count other))
    (assoc this (first other) (last other)))
    ;(cons (.iterator this) other))
  (empty [this] (empty? index))
  (equiv [this other] (.. this (equals other)))

  clojure.lang.ILookup
  (valAt [this i] (.. this (valAt i nil)))
  (valAt [this i or-else] (if-let [n (get (. this lookup) i)]
                            (nth (. this values) n)
                            or-else))

  clojure.lang.Associative
  (containsKey [this key]
    (contains? lookup key))
  (entryAt [this key]
    (MapEntry/create key (.. this (valAt key))))
  (assoc [this idx val]
    "Takes a key of the index type and map
    of column names to values and return a
    frame with a new row added corresponding
    to the input index and column map."
    (if (contains? this idx)
      (series (assoc values (get lookup idx) val)
              index)
      (series (conj values val) (conj index idx)))))

; Constructor
(defn series

  ([data] (series data (range (count data))))

  ([data index]

   (let [data (vec data)
         index (vec index)
         lookup (into {} (enumerate index false))]

     (assert (apply distinct? index))
     (assert (= (count data) (count index)))
     (assert (apply = (map type (filter (comp not nil?) data))))

     (Series. data index lookup))))


(defmethod print-method Series [^Series srs writer]
  (.write writer (str (class srs)
                      "\n"
                      (str/join "\n"
                                (map
                                  (fn [[i d]]
                                    (str i " " (if (nil? d) "nil" d)))
                                  (zip (. srs index) (. srs values)))))))


(defn series?
  [x]
  (instance? Series x))

(defn index
  [^Series srs]
  (. srs index))

(defn values
  [^Series srs]
  (. srs values))

(defn ix
  "Takes a series and an index and returns
  the item in the series corresponding
  to the input index"
  ([^Series srs i] (get srs i nil))
  ([^Series srs i or-else] (get srs i or-else)))


(defn set-index
  "Return a series with the same values
  but with the updated index."
  [^Series srs index]
  (series (values srs) (vec index)))


(defn mapvals
  "Apply the function to all vals in the Series,
  returning a new Series consistening of these
  transformed vals with their indices."
  [^Series srs f]
  (series (map f (values srs)) (index srs)))


(defn select
  "Takes a series and a list of possibly-true values
  and return a series containing only vals that
  line up to truthy values"
  [^Series srs selection]

  (assert (= (count srs) (count selection)))

  (let [selection (if (series? selection) (values selection) selection)
        to-keep (for [[keep? [idx val]] (zip selection srs)
                      :when keep?]
                  [idx val])
        idx (map #(nth % 0) to-keep)
        vals (map #(nth % 1) to-keep)]

    (series vals idx)))


(defn subset
  "Return a subseries defined
  the start and end indices (which are
  integer like) using the index order.

  The subset is inclusive on the start
  but exclusive on the end, meaning that
  (subset srs 0 (count srs)) returns the
  same series"
  [^Series srs start end]

  (assert (<= start end))

  (let [last (count srs)
        srs-begin (min (max 0 start) last)
        srs-end (min (max 0 end) last)]

    (series
      (subvec (values srs) srs-begin srs-end)
      (subvec (index srs) srs-begin srs-end)
      )))


(defn head
  "Return a subseries consisting of the
  first n elements of the input series
  using the index order.

  If n > (count srs), return the
  whole series."
  ([^Series srs] (head srs 5))
  ([^Series srs n] (subset srs 0 n)))


(defn tail
  "Return a subseries consisting of the
  last n elements of the input series
  using the index order.

  If n > (count srs), return the
  whole series."
  ([^Series srs] (tail srs 5))
  ([^Series srs n]
   (let [start (- (count srs) n)
         end (count srs)]
     (subset srs start end))))


(defn ^{:protected true}  nillify
  "Takes a binary function and returns
  a function that short-circuits nil values."
  [f]
  (fn [x y]

    (cond
      (nil? x) nil
      (nil? y) nil
      :else (f x y))))

(defn ^{:protected true} broadcast
  [f]
  (fn [x y]
    (cond
      (and (instance? Series x) (instance? Series y)) (do
                                                        (assert (= (index x) (index y)))
                                                        (series (for [[l r] (zip (values x) (values y))]
                                                                  (f l r))
                                                                (index x)))
      (instance? Series x) (series (for [l (values x)]
                                     (f l y))
                                   (index x))
      (instance? Series y) (series (for [r (values y)]
                                     (f x r))
                                   (index y))
      :else (f x y))))

(defn ^{:protected true} multi-broadcast
  [f]
  (fn [x & args]

    (loop [x x
           args args]

      (if (empty? args)
        x
        (recur ((broadcast f) x (first args))
               (rest args))))))

(def lt (broadcast (nillify <)))
(def lte (broadcast (nillify <=)))
(def gt (broadcast (nillify >)))
(def gte (broadcast (nillify >=)))

(def add (multi-broadcast (nillify +)))
(def sub (multi-broadcast (nillify -)))
(def mul (multi-broadcast (nillify *)))
(def div (multi-broadcast (nillify /)))

(def eq (multi-broadcast (nillify =)))
(def neq (multi-broadcast (comp not (nillify =))))



