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
(deftype ^{:protected true} Series [data index lookup]

  clojure.lang.ILookup
  (valAt [_ k] (get lookup k))
  (valAt [_ k or-else] (get lookup k or-else)))

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
;
;
;(defn filter
;  [srs & args]
;  (let [filters (map #(every? boolean %) (zip args))
;        data (:data srs)
;        index (:index srs)
;        data-filtered (vec (for [[keep? item] (zip filters data) :when keep?] item))
;        index-filtered (vec (for [[keep? item] (zip filters index) :when keep?] item))]
;    (series data-filtered :index index-filtered :name (:name srs))))