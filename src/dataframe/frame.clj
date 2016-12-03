(ns dataframe.frame
  (:refer-clojure)
  (:require [dataframe.series :as series]
            [clojure.string :as str]
            [dataframe.series :as series]
            [dataframe.util :refer :all])
  (:import (java.util Map)))


(declare frame
         assoc-index
         iterrows
         rows->vectors
         set-index
         -seq->frame
         -list-of-row-maps->frame
         -list-of-index-row-pairs->frame
         -map->frame
         -map-of-series->frame
         -map-of-sequence->frame)


; A Frame can be interpreted as:
; - A Map of index keys to maps of values
; - A Map of column names to Series as columns
;
; A Frame supports
; - Order 1 lookup of row maps by index key
; - Order 1 lookup of [index row] pairs by position (nth)
; - Order 1 lookup of columns by name
;
; A Frame does not guarantee column order
;
; As viewed as a Clojure PersistentColleection, it is a
; collection of [index row] pairs, where a row is a map
; of [column val] pairs (for the purpose of seq and cons).
; As viewed as an association, it is a map from index
; keys to row maps.
(deftype ^{:protected true} Frame [index column-map]

  java.lang.Object
  (equals [this other]
    (cond (nil? other) false
          (not (= Frame (class other))) false
          :else (and
                  (= (. this index) (. other index))
                  (= (. this column-map) (. other column-map)))))
  (hashCode [this]
    (hash [(hash (. this index)) (hash (. this column-map))]))

  java.lang.Iterable
  (iterator [this]
    (.iterator (iterrows this)))

  clojure.lang.Counted
  (count [this] (count index))

  clojure.lang.IPersistentCollection
  (seq [this] (if (empty? index)
                nil
                (iterrows this)))
  (cons [this other]
    "Takes a vector pair of [idx row],
    where row is a map, and returns a
    Frame extended by one row."
    (assert vector? other)
    (assert 2 (count other))
    (assert map? (last other))
    (let [[idx m] other]
      (assoc-index this idx m)))
  (empty [this] (empty? index))
  (equiv [this other] (.. this (equals other))))


; It has an index for row-wise lookups
(defn frame
  "Create a Frame from on of the following inputs:

  - A map of column keys to sequences representing column values
  - A map of column keys to Series reprsenting column values
  - A sequence of index keys to maps representing rows
  "
  ([data index] (set-index (frame data) index))
  ([data]
   (cond
     (map? data) (-map->frame data)
     (seq? data) (-seq->frame data)
     (vector? data) (-seq->frame data)
     :else (throw (new Exception "Encountered unexpected type for frame constructor")))))

(defn ^{:protected true} -map->frame
  [^Map data-map]

  ; Ensure all values have the same length
  (if (not (empty? data-map))
    (assert (apply = (map count (vals data-map)))))

  (let [k->srs (into {}
                     (for [[k xs] data-map]
                       (if (series/series? xs)
                         [k xs]
                         [k (series/series xs)])))]

    (-map-of-series->frame k->srs)))


(defn ^{:protected true} -map-of-series->frame
  "Takes a map of column keys to Series objects
  representing column values.
  Return a Frame."
  [map-of-srs]

  ; Assert all the indices are aligned
  (if (not (empty? map-of-srs))
    (assert (apply = (map series/index (vals map-of-srs)))))

  (if (empty? map-of-srs)
    (Frame. [] {})

    (let [any-index (series/index (nth (vals map-of-srs) 0))]
      (Frame. any-index map-of-srs))))


(defn ^{:protected true} -seq->frame
  "Take a list of either maps
  (each representing a row)
  or pairs of index->maps.
  Return a Frame."
  [s]
  (if (map? (first s))
    (-list-of-row-maps->frame s)
    (-list-of-index-row-pairs->frame s)))


(defn ^{:protected true} -list-of-row-maps->frame
  "Take a list of maps (each representing a row
  with keys as columns and vals as row values)
  and return a Frame"
  [row-maps]
  (let [index (range (count row-maps))
        columns (into #{} (flatten (map keys row-maps)))
        col->vec (into {} (for [col columns]
                            [col (vec (map #(get % col nil) row-maps))]))
        col->srs (into {} (for [[col vals] col->vec]
                            [col (series/series vals index)]))]

    (-map-of-series->frame col->srs)))


(defn ^{:protected true} -list-of-index-row-pairs->frame
  "Take a list of pairs
  of index values to row-maps
  and return a Frame."
  [seq-of-idx->maps]

  (let [index (into [] (map first seq-of-idx->maps))
        row-maps (map last seq-of-idx->maps)
        columns (into #{} (flatten (map keys row-maps)))
        col->vec (into {} (for [col columns]
                            [col (vec (map #(get % col nil) row-maps))]))
        col->srs (into {} (for [[col vals] col->vec]
                   [col (series/series vals index)]))]

    (-map-of-series->frame col->srs)))


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

(defn assoc-index
  "Takes a key of the index type and map
   of column names to values and return a
    frame with a new row added corresponding
    to the input index and column map."
  [^Frame df i row-map]

  (assert map? row-map)

  (let [new-columns (into {}
                          (for [[k srs] (column-map df)]
                            [k (conj srs [i (get row-map k nil)])]))
        new-index (conj (index df) i)]
    (frame new-columns new-index)))


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
  "Return an iterator key-val pairs
  of index values to row values (as a vector)"
  [df]
  (zip
    (index df)
    (apply zip (map series/values (vals (column-map df))))))

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


(defn select
  [^Frame df selection]

  (assert (= (count df) (count selection)))

  (let [selection (if (series/series? selection) (series/values selection) selection)
        to-keep (for [[keep? [idx row-map]] (zip selection df)
                      :when keep?]
                  [idx row-map])

        idx (map #(nth % 0) to-keep)
        vals (map #(nth % 1) to-keep)]

    (frame vals idx)))


(defmacro with-context
  "Takes a context (map-like object)
  and a list of body expression forms
  and evaluates the forms by replacing
  all symbols starting with $
  with values from the context corresponding
  to keywords of the same name.

  So, when it encounters:
  $x
  it replaces it with the value of looking up:
  :x

  in the context map.

  In particular, this can be used with
  a Frame as the context."
  [ctx & body]
  (let [replace-fn (fn [expr]
                     (clojure.walk/postwalk
                       (fn [x]
                         (if (and
                               (symbol? x)
                               (clojure.string/starts-with? (name x) "$"))
                           `(col ~ctx ~(keyword (subs (name x) 1)))
                           x))
                       expr))
        exprs (map replace-fn body)]

    `(do ~@exprs)))


(defmacro with->
  [df & exprs]
  `(with-context ~df
                (-> ~df ~@exprs)))