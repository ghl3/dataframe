(ns dataframe.frame
  (:require [dataframe.series :as series]))


; A Frame contains a map of column names to
; Series objects holding the underlying data
; as well as an index
(defrecord ^{:protected true} Frame [data index])


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
        index (if index index (range nrows))]


    (Frame. (into (sorted-map) (map
                                 (fn [[col-name col-data]]
                                  [col-name (series/series col-data :index index :name col-name)])
                                data-map))
            index)))

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


(defn col
  "Return the column from the dataframe
  by the given name as a Series"
  [df col-name]
  (-> df :data col-name))
