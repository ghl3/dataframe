(ns dataframe.pipeline-test
  (:refer-clojure :exclude [group-by])
  (:require [dataframe.core :refer :all]
            [expectations :refer [expect]]))

(expect (frame {:a [1 2 3]
                :b [10 20 30]
                :c [6 7 8]
                :d [16 27 38]})
  (with-> (frame {:a [1 2 3] :b [10 20 30]})
    (assoc-col :c (add $a 5))
    (assoc-col :d (add $b $c))))

(expect (frame {:c [3 6] :b [2 4] :a [1 2]} [:x :y])
  (let [df (frame [[:x {:a 1 :b 2}]
                   [:y {:a 2 :b 4}]
                   [:z {:a 3 :b 8}]])]
    (with-> df
      (select (lte $a 2))
      (assoc-col :c (add $a $b))
      (sort-rows :c :b))))

(expect (frame {:foo [8 8 14] :bar [0 -2 -3]} [:w :y :z])
  (let [df (frame [[:w {:a 0 :b 8}]
                   [:x {:a 1 :b 2}]
                   [:y {:a 2 :b 4}]
                   [:z {:a 3 :b 8}]])]
    (with-> df
      (select (and (lte $a 2) (gte $b 4)))
      (assoc-col :c (add $a $b))
      (maprows->df (fn [row] {:foo (+ (:a row) (:c row))
                              :bar (- (:b row) (:c row))}))
      head)))