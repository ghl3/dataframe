(ns dataframe.frame
  (:refer-clojure :exclude [nth])
  (:require [dataframe.series :as series]
            [clojure.string :as str]
            [dataframe.util :refer :all]))


(declare iterrows)

; A Frame contains a map of column names to
; Series objects holding the underlying data
; as well as an index
(defrecord ^{:protected true} Frame [data index columns])


; It has an index for row-wise lookups
(defn frame
  [data-map & {:keys [index]}]

  ; Cannot have a key named 'index'
  (assert (nil? (:index data-map)))

  ; Ensure all values have the same length
  (assert (apply = (map count (vals data-map))))

  ; If there is an index, ensure it's unique
  (if index (assert (apply distinct? index)))

  (let [nrows (-> data-map vals first count)
        index (vec (if index index (range nrows)))]


    (Frame. (into (sorted-map) (map
                                 (fn [[col-name col-data]]
                                  [col-name (series/series col-data :index index :name col-name)])
                                data-map))
            index
            (keys data-map))))


(defmethod print-method Frame [df writer]
  (.write writer (str (class df)
                      "\n"
                      \tab (str/join \tab (:columns df))
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
  (if (get (:index df) i)
    (let [vals (map #(series/ix % i) (-> df :data vals))]
      (series/series vals :index (-> df :data keys) :name i))
    nil))


(defn nth
  "Get the 'row' of the input dataframe
  corresponding to the input index.

  The 'row' is a Series corresponding to the
  input index applied to every column
  in this dataframe, where the index of
  the new series are the column names.

  If no row matching the index exists,
  return nil
  "
  [df n]
  (if (< n (count (:index df)))
    (let [vals (map #(series/nth % n) (-> df :data vals))]
      (series/series vals :index (-> df :data keys) :name (get (:index df) n)))
    nil))


(defn col
  "Return the column from the dataframe
  by the given name as a Series"
  [df col-name]
  (-> df :data col-name))


(defn iterrows
  "Return an iterator over vectors
  of key-val pairs of the row's
  index value and the value of that
  row as a vector"
  [df]
  (zip
    (:index df)
    (apply zip (map :data (vals (:data df))))))

