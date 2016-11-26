(ns dataframe.frame-test
  (:require [dataframe.frame :as frame :refer [index]]
            [expectations :refer [expect more-of]]
            [dataframe.series :as series]))


(expect '(0 1 2)
        (let [df (frame/frame {:a '(1 2 3) :b '(2 4 6)})]
          (index df)))


(expect (series/series '(1 2 3) '(0 1 2))
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)})
            (frame/col :a)))


(expect nil
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)})
            (frame/col :c)))


(expect (series/series [1 2] [:a :b])
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
            (frame/ix :x)))


(expect '([:x [1 2]] [:y [2 4]] [:z [3 6]])
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
            frame/iterrows))

