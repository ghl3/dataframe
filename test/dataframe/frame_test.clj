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


(expect '( [:x {:a 1 :b 2}]
           [:y {:a 2 :b 4}]
           [:z {:a 3 :b 6}])
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
            frame/iterrows))


(expect (series/series [3 6 9] [:x :y :z])
        (frame/maprows
          (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
          (fn [row] (+ (:a row) (:b row)))))