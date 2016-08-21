(ns dataframe.frame-test
  (:require [dataframe.frame :as frame]
            [expectations :refer [expect]]
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
