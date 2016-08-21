(ns dataframe.series-test
  (:require [dataframe.series :as srs]
            [expectations :refer [expect]]))


(expect 1
        (let [my-srs (srs/series '(1 2 3) '("A" "B" "C"))]
          (srs/ix my-srs "A")))

(expect nil
        (let [my-srs (srs/series '(1 2 3) '("A" "B" "C"))]
          (srs/ix my-srs "D")))


(expect AssertionError
        (let [my-srs (srs/series '(1 2 3) '("A" "B" "A"))]
          (srs/ix my-srs "D")))