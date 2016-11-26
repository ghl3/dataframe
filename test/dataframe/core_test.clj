(ns dataframe.core-test
  (:require [clojure.test :refer :all]
            [dataframe.core :refer :all]
            [dataframe.frame :as frame]))


;
;
;(expect (more-of df
;
;                 )
;
;        (let [df (frame/frame {:a '(1 2 3) :b '(2 4 6)})
;              a-min (with-df-> f (frame/filter :a (< 10)