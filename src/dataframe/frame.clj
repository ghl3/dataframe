(ns dataframe.frame
  (:refer-clojure)
  (:require [dataframe.series :as series]
            [clojure.string :as str]
            [dataframe.series :as series]
            [dataframe.util :refer :all])
  (:import (dataframe.series Series)
           (java.util Map)))


(declare rows->vectors
         set-index
         -series-map->frame
         -sequence-map->frame)

; A Frame contains a map of column names to
; Series objects holding the underlying data
; as well as an index
;(defrecord ^{:protected true} Frame [index columns])


; A Frame is a map of column names to Series
; objects, where each Series must have
; matching indices
(deftype ^{:protected true} Frame [index column-map]

  ; Return the colunn corresponding to the
  ; given key
  clojure.lang.ILookup
  (valAt [_ k] (get column-map k))
  (valAt [_ k or-else] (get column-map k or-else)))


; It has an index for row-wise lookups
(defn frame
  "Takes a map of column names to columns.
  A Column may be one of:
  - A Series
  - An iterable
  but all columns in the map must be one
  or the other."
  ([^Map data-map]

  ; Ensure all values have the same length
  (assert (apply = (map count (vals data-map))))

  (let [type-list (map type (vals data-map))
        is-series (map #(= Series %) type-list)]

    (cond
      (every? true? is-series) (-series-map->frame data-map)
      (not-any? true? is-series) (-sequence-map->frame data-map)
      :else (throw (new Exception "Mixed Series/Non-Series input to frame")))))

  ([^Map data-map index]
   (set-index (frame data-map) index)))


(defn -series-map->frame
  [srs-map]

  ; Assert all the indices are aligned
  (assert (apply = (map series/index (vals srs-map))))

  (if (empty? srs-map)
    (Frame. [] {})

    (let [any-index (series/index (nth (vals srs-map) 0))]
      (Frame. any-index srs-map))))


(defn -sequence-map->frame
  [seq-map]

  (-series-map->frame
    (into {}
          (for [[col seq] seq-map]
            [col (series/series seq)]))))


(defn index
  [^Frame frame]
  (. frame index))


(defn column-map
  [^Frame frame]
  (. frame column-map))

(defn columns
  [^Frame frame]
  (keys (column-map frame)))


(defn set-index
  [^Frame frame index]
  (Frame. index (into {} (for [[col srs] (column-map frame)]
                               [col (series/set-index srs index)]))))

(defmethod print-method Frame [df writer]

  (.write writer (str (class df)
                      "\n"
                      \tab (str/join \tab (columns df))
                      "\n"
                      (str/join "\n" (map
                                       (fn [[idx row]] (str idx \tab (str/join \tab row)))
                                       (rows->vectors df))))))


(defn ix
  "Get the 'row' of the input dataframe
  corresponding to the input index.

  The 'row' is a Series corresponding to the
  input index applied to every column
  in this dataframe, where the index of
  the new series are the column names.

  If no row matching the index exists,
  return nil
  "
  [df i]
  (if (some #(= i %) (index df))
    (series/series (map #(series/ix % i) (-> df column-map vals)) (-> df column-map keys))
    nil))


(defn col
  "Return the column from the dataframe
  by the given name as a Series"
  [df col-name]
  (-> df column-map col-name))


(defn rows->vectors
  "Return an iterator over vectors
  of key-val pairs of the row's
  index value and the value of that
  row as a vector"
  [df]
  (zip
    (index df)
    (apply zip (map series/data (vals (column-map df))))))



(defn iterrows
  "Return an iterator over vectors
  of key-val pairs of the row's
  index value and the value of that
  row as a map"
  [df]
  (for [idx (index df)]
    [idx (into {} (for [[col srs] (column-map df)]
                    [col (series/ix srs idx)]))]))


(defn maprows
  "Apply the function to all vals in the Series,
  returning a new Series consistening of these
  transformed vals with their indices."
  [^Frame df f]

  (let [rows (for [[_ row] (iterrows df)]
               (f row))]

    (series/series rows (index df))))
