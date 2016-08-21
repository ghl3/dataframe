(ns dataframe.frame-test
  (:require [dataframe.frame :as frame]
            [expectations :refer [expect more-of]]
            [dataframe.series :as series]))


(expect '(0 1 2)
        (let [df (frame/frame {:a '(1 2 3) :b '(2 4 6)})]
          (:index df)))


(expect (series/series '(1 2 3) :index '(0 1 2) :name :a)
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)})
            (frame/col :a)))


(expect nil
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)})
            (frame/col :c)))


(expect (more-of srs
                 :x (:name srs)
                 [1 2] (::series/data srs))
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)} :index [:x :y :z])
            (frame/nth 0)))


(expect '([:x [1 2]] [:y [2 4]] [:z [3 6]])
        (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)} :index [:x :y :z])
            frame/iterrows))

