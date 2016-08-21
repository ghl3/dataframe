(ns dataframe.series
  (:require [clojure.core.matrix :as matrix]
            [dataframe.util :refer :all]
            [clojure.string :as str]))


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
(defrecord ^{:private true} Series [data index])


(defmethod print-method Series [srs writer]
  (.write writer (str/join "\n" (map (fn [[i d]] (str i " " d)) (zip (:index srs) (:data srs))))))


(defn series
  [data index & {:keys [name]} ]

  (assert (apply distinct? index))

  ; TODO: Infer when we can leverage a matrix
  (let [data-as-matrix (vec data)
        index-lookup (delay (into {} (enumerate index false)))
        srs (Series. data-as-matrix index)]

    (cond-> srs
            name (assoc :name name)
            true (assoc :index-lookup index-lookup))))


(defn ix
  "Takes a series and an index and returns
  the item in the series corresponding
  to the input index"
  [srs i]
  (let [position (get @(:index-lookup srs) i)]
    (get (:data srs) position)))


