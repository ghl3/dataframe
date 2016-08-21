(ns dataframe.series-test
  (:require [dataframe.series :as srs]
            [expectations :refer [expect]]))


(expect '(0 1 2)
        (let [my-srs (srs/series '(:x :y :z))]
          (:index my-srs)))

(expect 1
        (let [my-srs (srs/series '(1 2 3) :index '("A" "B" "C"))]
          (srs/ix my-srs "A")))

(expect nil
        (let [my-srs (srs/series '(1 2 3) :index '("A" "B" "C"))]
          (srs/ix my-srs "D")))


(expect AssertionError
        (let [my-srs (srs/series '(1 2 3) :index '("A" "B" "A"))]
          (srs/ix my-srs "D")))

(expect {:a 1 :b 2 :c 3}
        (-> (srs/series '(1 2 3) :index '(:a :b :c))
            srs/srs->map))