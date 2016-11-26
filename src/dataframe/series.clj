(ns dataframe.series
  (:refer-clojure :exclude [nth])
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
(deftype ^{:protected true} Series [data index lookup]

  java.lang.Object
  (equals [this other]
    (cond (nil? other) false
          (not (= Series (class other))) false
          :else (every? true?
                        [(= (. this data ) (. other data))
                         (= (. this index ) (. other index))])))

  clojure.lang.ILookup
  (valAt [_ k] (get lookup k))
  (valAt [_ k or-else] (get lookup k or-else))

  java.lang.Iterable
  (iterator [this]
    (.iterator (zip (index data))))

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
                                (map (fn [[i d]] (str i " " d)) (zip (. srs index) (. srs data)))))))


(defn index
  [^Series srs]
  (. srs index))

(defn data
  [^Series srs]
  (. srs data))

(defn ix
  "Takes a series and an index and returns
  the item in the series corresponding
  to the input index"
  [^Series srs i]
  (let [position (get (. srs lookup) i)]
    (get (. srs data) position)))


(defn srs->map
  [^Series srs]
  (into (sorted-map) (zip (. srs index) (. srs data))))


(defn set-index
  [^Series srs index]
  (series (data srs) index))