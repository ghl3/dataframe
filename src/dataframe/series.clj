(ns dataframe.series
  (:refer-clojure)
  (:require [clojure.core.matrix :as matrix]
            [dataframe.util :refer :all]
            [clojure.string :as str]
            [clojure.core :as core]))

; TODO: Use matrix
;(matrix/set-current-implementation :vectorz)

; A 1-d vector of data with an associated
; index of the same length.
;
; All items in the index must be unique.
;
; https://gist.github.com/david-mcneil/1684980
;
(deftype ^{:protected true} Series [values index lookup]

  java.lang.Object
  (equals [this other]
    (cond (nil? other) false
          (not (= Series (class other))) false
          :else (every? true?
                        [(= (. this values) (. other values))
                         (= (. this index ) (. other index))])))
  (hashCode [this]
    (hash [(hash (. this index)) (hash (. this values))]))

  clojure.lang.ILookup
  (valAt [_ k] (get lookup k))
  (valAt [_ k or-else] (get lookup k or-else))

  java.lang.Iterable
  (iterator [this]
    (.iterator (zip index values)))


  clojure.lang.Counted
  (count [this] (count index))

  )

; Constructor
(defn series

  ([data] (series data (range (count data))))

  ([data index]

   (let [data (vec data)
         index (vec index)
         lookup (into {} (enumerate index false))]

     (assert (apply distinct? index))
     (assert (= (count data) (count index)))
     (assert (apply = (map type data)))

     (Series. data index lookup))))


(defmethod print-method Series [^Series srs writer]
  (.write writer (str (class srs)
                      "\n"
                      (str/join "\n"
                                (map (fn [[i d]] (str i " " d)) (zip (. srs index) (. srs values)))))))


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
  [^Series srs i]
  (let [position (get (. srs lookup) i)]
    (get (. srs values) position)))

(defn mapvals
  "Apply the function to all vals in the Series,
  returning a new Series consistening of these
  transformed vals with their indices."
  [^Series srs f]

  (series (map f (values srs)) (index srs)))


(defn srs->map
  [^Series srs]
  (into (sorted-map) (zip (. srs index) (. srs values))))


(defn set-index
  [^Series srs index]
  (series (values srs) index))


(defn select
  "Takes a series and a list of possibly-true values
  and return a series containing only vals that
  line up to truthy values"
  [^Series srs selection]

  (assert (= (count srs) (count selection)))

  (let [to-keep (for [[keep? [idx val]] (zip selection srs)
                      :when keep?]
                  [idx val])

        idx (map #(nth % 0) to-keep)
        vals (map #(nth % 1) to-keep)]

    (series vals idx)))



; Operators


(defn broadcast
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


(def lt (broadcast <))
(def lte (broadcast <=))

(def gt (broadcast >))
(def gte (broadcast >=))

(defn plus
  [x & args]

  (if (or (empty? args) (nil? args))
    x
    (apply plus (concat [((broadcast +) x (first args))] (rest args)))))
