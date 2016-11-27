(ns dataframe.frame
  (:refer-clojure)
  (:require [dataframe.series :as series]
            [clojure.string :as str]
            [dataframe.series :as series]
            [dataframe.util :refer :all])
  (:import (dataframe.series Series)
           (java.util Map)))


(declare iterrows
         rows->vectors
         set-index
         -list->frame
         -map->frame
         -map-of-series->frame
         -map-of-sequence->frame)

; A Frame contains a map of column names to
; Series objects holding the underlying data
; as well as an index
;(defrecord ^{:protected true} Frame [index columns])


; A Frame is a map of column names to Series
; objects, where each Series must have
; matching indices
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

  ; Return the colunn corresponding to the
  ; given key
  clojure.lang.ILookup
  (valAt [_ k] (get column-map k))
  (valAt [_ k or-else] (get column-map k or-else))

  java.lang.Iterable
  (iterator [this]
    (.iterator (iterrows this)))

  clojure.lang.Counted
  (count [this] (count index))

  )


; It has an index for row-wise lookups
(defn frame
  "Takes a map of column names to columns.
  A Column may be one of:
  - A Series
  - An iterable
  but all columns in the map must be one
  or the other."
  ([data index] (set-index (frame data) index))
  ([data]
   (cond
     (map? data) (-map->frame data)
     (seq? data) (-list->frame data)
     (vector? data) (-list->frame data)
     :else (throw (new Exception "Encountered unexpected type for frame constructor")))))

(defn -map->frame
  [^Map data-map]

  ; Ensure all values have the same length
  (assert (apply = (map count (vals data-map))))

  (let [type-list (map type (vals data-map))
        is-series (map #(= Series %) type-list)]

    (cond
      (every? true? is-series) (-map-of-series->frame data-map)
      (not-any? true? is-series) (-map-of-sequence->frame data-map)
      :else (throw (new Exception "Mixed Series/Non-Series input to frame")))))

(defn -map-of-series->frame
  [map-of-srs]

  ; Assert all the indices are aligned
  (assert (apply = (map series/index (vals map-of-srs))))

  (if (empty? map-of-srs)
    (Frame. [] {})

    (let [any-index (series/index (nth (vals map-of-srs) 0))]
      (Frame. any-index map-of-srs))))


(defn -map-of-sequence->frame
  [map-of-seq]

  (-map-of-series->frame
    (into {}
          (for [[col seq] map-of-seq]
            [col (series/series seq)]))))


(defn -list->frame
  [seq-of-maps]

  (let [map-of-sequences (new java.util.HashMap)]

    (doall (for [mp seq-of-maps
                 [k v] mp]

             (do
               (if (not (.. map-of-sequences (containsKey k)))
                 (.. map-of-sequences (put k (new java.util.ArrayList))))
             (.. map-of-sequences (get k) (add v)))))

    (-map-of-sequence->frame map-of-sequences)))



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
                           `(get ~ctx ~(keyword (subs (name x) 1)))
                           x))
                       expr))
        exprs (map replace-fn body)]

    `(do ~@exprs)))


(defmacro with->
  [df & exprs]
  `(with-context ~df
                (-> ~df ~@exprs)))