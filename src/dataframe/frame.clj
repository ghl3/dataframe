(ns dataframe.frame
  (:refer-clojure)
  (:require [dataframe.series :as series]
            [clojure.string :as str]
            [dataframe.series :as series]
            [dataframe.util :refer :all]
            [clojure.set :as set])
  (:import (java.util Map)
           (dataframe TableBuilder)))

(declare frame
         assoc-ix
         assoc-col
         iterrows
         columns
         print-row
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
(deftype Frame [index column-map]

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
  ;Takes a vector pair of [idx row],
  ;where row is a map, and returns a
  ;Frame extended by one row."
  (cons [this other]
    (assert vector? other)
    (assert (= 2 (count other)))
    (assert map? (last other))
    (let [[idx m] other]
      (assoc-ix this idx m)))
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


(defmethod print-method Frame
  [df writer]
  (.write writer (str (class df) "\n"))
  (.write writer
          (let [table (new TableBuilder "" (columns df))]
            (doall (for [[idx row] (rows->vectors df)]
                     (. table (addRow idx row))))
            (. table toString))))



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

(defn assoc-ix
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

(defn assoc-col
  "Takes a key of the index type and map
   of column names to values and return a
    frame with a new row added corresponding
    to the input index and column map."
  [^Frame df col-name col]

  (let [col (if (series/series? col)
              col
              (series/series col (index df)))]
    (frame (assoc (column-map df) col-name col)
      (index df))))


(defn ^{:protected true} print-row
  [row]
  (str/join
    \tab
    (into []
          (map #(if (nil? %) "nil" %) row))))


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
  (get (column-map df) col-name))

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

(defn maprows->srs
  "Apply the function to each row in the DataFrame
  (where the representation of each row is a map of
  column names to values).
  Return a Series whose index is the index of the
  original DataFrame and whose value is the value
  of the applied function."
  [^Frame df f]
  (let [rows (for [[_ row] (iterrows df)]
               (f row))]
    (series/series rows (index df))))

(defn maprows->df
  "Apply the function to each row in the DataFrame
  (where the representation of each row is a map of
  column names to values).  The function should return
  a Map.
  Return a DataFrame whose index is the same as the
  original dataframe and whose columns are the values
  of the maps returned by the function."
  [^Frame df f]
  (let [rows (for [[idx row] (iterrows df)]
               [idx (f row)])]
    (-list-of-index-row-pairs->frame rows)))

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

(defn subset
  "Return a subset of the input Frame
  the start and end indices (which are
  integer like) using the index order.

  The subset is inclusive on the start
  but exclusive on the end, meaning that
  (subset srs 0 (count srs)) returns the
  same series"
  [^Frame df start end]

  (assert (<= start end))

  (let [last (count df)
        srs-begin (min (max 0 start) last)
        srs-end (min (max 0 end) last)
        subset-index (subvec (index df) srs-begin srs-end)
        subset-columns (into {} (for [[name col] (column-map df)]
                                  [name (series/subset col start end)]))]
    (frame subset-columns subset-index)))

(defn head
  "Return a subseries consisting of the
  first n elements of the input frame
  using the index order.

  If n > (count df), return the
  whole frame."
  ([^Frame df] (head df 5))
  ([^Frame df n] (subset df 0 n)))

(defn tail
  "Return a subseries consisting of the
  last n elements of the input frame
  using the index order.

  If n > (count df), return the
  whole frame."
  ([^Frame df] (tail df 5))
  ([^Frame df n]
    (let [start (- (count df) n)
          end (count df)]
      (subset df start end))))

(defn sort-rows
  "Sort DataFrame rows using the
  given column names in the order
  that they appear"
  [^Frame df & col-names]
  (let [get-sort-key (fn [[idx row-map]]
                       (into [] (for [col col-names] (get row-map col))))
        sorted-idx-row-pairs (sort-by get-sort-key df)]
    (-list-of-index-row-pairs->frame sorted-idx-row-pairs)))


(defn add-suffix
  "Add a suffix to a name.
  A name or suffix can be either
  a string or a keyword"
  [col-name suffix]
  (cond
    (string? col-name) (str col-name (name suffix))
    (keyword? col-name) (keyword (str (name col-name) (name suffix)))
    :else (throw (new Exception))))


(defn assoc-common-column
  "Conditionally associated the input
  column and value pair into the input
  map.  The name associated in and whether
  the association happens at all is dependent
  on the common-col-resolution map, using
  whether the columns is left? or not."
  [into-map col-name val left? common-columns
   {:keys [suffixes prefer-column]
    :or   {suffixes ["-x" "-y"]
           prefer-column nil}}]

  (assert (contains? #{:left :right nil} prefer-column))

  (if (not (contains? common-columns col-name))
    (assoc into-map col-name val)

    (if prefer-column

      (case [prefer-column left?]
        [:left true] (assoc into-map col-name val)
        [:left false] into-map
        [:right false] (assoc into-map col-name val)
        [:right true] into-map
        :else (throw (new Exception)))

      (let [[left-suffix right-suffix] suffixes
            suffix (if left? left-suffix right-suffix)]

        (assoc into-map (add-suffix col-name suffix) val)))))


(defn join-index
  [left-index right-index join-type]
  (case join-type
    :left  left-index
    :right right-index
    :outer (concat left-index (filter #(not (contains? (set left-index) %)) right-index))
    :inner (filter #(contains? (set right-index) %) left-index)
    :else  (throw (new Exception))))


(defn join
  [^Frame left ^Frame right
   & {:keys [how suffixes prefer-column]
      :or   {how :inner
             suffixes ["-x" "-y"]
             prefer-column nil}
      :as kwargs}]

  (let [shared-cols (into #{} (set/intersection (set (columns left)) (set (columns right))))
        idx (join-index (index left) (index right) how)
        left-cols  (for [[col srs] (column-map left)]  [col (series/series (map #(series/ix srs %) idx) idx) true])
        right-cols (for [[col srs] (column-map right)] [col (series/series (map #(series/ix srs %) idx) idx) false])
        all-cols (concat left-cols right-cols)
        col-map (reduce (fn [coll [col-name val left?]]
                          (assoc-common-column coll col-name val left? shared-cols kwargs))
                        {}
                        all-cols)]
    (frame col-map idx)))

(defn group-by-fn
  "Group a Frame by the given function,
  which must be a function of it's a row-map,
  and return a map of fn vals to Frames"
  [^Frame df f]
  (let [grouped-idx-rows (group-by (fn [[idx row]] (f row)) (iterrows df))]
    (into {} (for [[k idx-row-list] grouped-idx-rows]
               [k (frame idx-row-list)]))))

(defn replace-df-column
  "Takes a symbol representing a Frame
  and an expression.  Walks the expression
  and replaces symbols starting with '$'
  with an expression to get a column from the
  dataframe whose name is a keyword matching
  the '$' symbol.

  In other words, if the expression contains:

  $foo

  it is replaced by:

  (col df :foo)
  "
  [df expr]
  (clojure.walk/postwalk
    (fn [x]
      (if (and
            (symbol? x)
            (clojure.string/starts-with? (name x) "$"))
        `(col ~df ~(keyword (subs (name x) 1)))
        x))
    expr))

(defmacro with->
  "A threading macro intended to thread
  expressions on data frames.
  Automatically replaces symbols starting
  with '$' with columns from the last
  DataFrame that was encountered
  in the threading."
  [df & exprs]
  (if (empty? exprs)
    df
    (let [sym (gensym)
          head (replace-df-column df (first exprs))
          tail (rest exprs)]
      `(let [~sym (-> ~df ~head)] (with-> ~sym ~@tail)))))
