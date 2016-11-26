(ns dataframe.frame
  (:refer-clojure)
  (:require [dataframe.series :as series]
            [clojure.string :as str]
            [dataframe.series :as series]
            [dataframe.util :refer :all])
  (:import (dataframe.series Series)
           (java.util Map)))


(declare iterrows
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


;
;
;    (apply some? (map #(= Series (type %)) (values data-map)))
;
;
;
;  (let [nrows (-> data-map vals first count)
;        index (vec (if index index (range nrows)))
;        data (into (sorted-map) ((map identity seq)
;                                  (fn [[col-name col-data]]
;                                    [col-name (series/series col-data :index index :name col-name)])
;                                  data-map))]
;
;    (merge
;      (assoc (Frame. index (keys data-map)) ::data data)
;      data)))
;
;
;
;
;(defn- map->srs-map
;  [mp name index]
;
;  (into (sorted-map)
;
;        (for [[k data] mp]
;
;          (if (instance? Series data)
;            (do
;              (assert (= (:index data) index))
;              (series/series (::series/data data)
;                      (series/series nil :index index :name nil)))))))
;


(defmethod print-method Frame [df writer]

  (.write writer (str (class df)
                      "\n"
                      \tab (str/join \tab (columns df))
                      "\n"
                      (str/join "\n" (map
                                       (fn [[idx row]] (str idx \tab (str/join \tab row)))
                                       (iterrows df))))))


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
      ;(series/series vals :index (-> df ::data keys) :name i))
    nil))

;
;(defn nth
;  "Get the 'row' of the input dataframe
;  corresponding to the input index.
;
;  The 'row' is a Series corresponding to the
;  input index applied to every column
;  in this dataframe, where the index of
;  the new series are the column names.
;
;  If no row matching the index exists,
;  return nil
;  "
;  [df n]
;  (if (< n (count (:index df)))
;    (let [vals (map #(series/nth % n) (-> df ::data vals))]
;      (series/series vals :index (-> df ::data keys) :name (get (:index df) n)))
;    nil))


(defn col
  "Return the column from the dataframe
  by the given name as a Series"
  [df col-name]
  (-> df column-map col-name))


(defn iterrows
  "Return an iterator over vectors
  of key-val pairs of the row's
  index value and the value of that
  row as a vector"
  [df]
  (zip
    (index df)
    (apply zip (map series/data (vals (column-map df))))))


;
;
;(defn filter
;  "Takes a DataFrame and a list of
;  iterables of the same length as
;  the DataFrame.
;
;  Returns all rows in the input DataFrame
;  for which all values for the input
;  args are truthy"
;  [df & args]
;
;  ;(let [filters (map and (zip args))] nil)
;
;
;
;  (let [filters (for [[k fn] (partition 2 kv-args)]
;                  (fn (col df k)))] nil))
;




;(defn map->df
;  "Takes a function, applies it to
;  each row, and returns a dataframe

;  [df fn]

