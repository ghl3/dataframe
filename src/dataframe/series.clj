(ns dataframe.series
  (:refer-clojure :exclude [nth])
  (:require [clojure.core.matrix :as matrix]
            [dataframe.util :refer :all]
            [clojure.string :as str]
            [clojure.core :as core]))


; TODO: Use matrix
;(matrix/set-current-implementation :vectorz)


; A 1-d list of data that is paired
; with an index of the same length.
; Items in a series can be obtained
; either by an index or by a position
;
; Indices may be of the following types:
;  - Numbers (int, floats, etc)
;  - Strings
;  - DataTime objects (Joda recommended)
;  - Vectors of any of the above
;
; Other types may work but are not currently
; recommended
;
; All items in the index must be unique.
;
; TODO: Make the index know it's internal position for fast lookup
(defrecord ^{:protected true} Series [data index])


(defmethod print-method Series [srs writer]
  (.write writer (str (class srs)
                      (if-let [name (:name srs)] (str ": " name) "")
                      "\n"
                      (str/join "\n"
                                (map (fn [[i d]] (str i " " d)) (zip (:index srs) (:data srs)))))))


(defn series
  [data & {:keys [index name]}]

  (if index (assert (apply distinct? index)))

  ; TODO: Infer when we can leverage a matrix
  (let [data-as-matrix (vec data)
        index (vec (if index index (range (count data))))
        index-lookup (into {} (enumerate index false))
        srs (Series. data-as-matrix index)]

    (cond-> srs
            name (assoc :name name)
            true (assoc :index-lookup index-lookup))))


(defn ix
  "Takes a series and an index and returns
  the item in the series corresponding
  to the input index"
  [srs i]
  (let [position (get (:index-lookup srs) i)]
    (get (:data srs) position)))


(defn nth
  "Takes a series and an integer and
  returns the "
  [srs n]
  (get (:data srs) n))


(defn srs->map
  [srs]
  (into (sorted-map) (zip (:index srs) (:data srs))))