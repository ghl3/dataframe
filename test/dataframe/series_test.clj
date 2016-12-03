(ns dataframe.series-test
  (:require [dataframe.series :as srs :refer [series index]]
            [expectations :refer [expect more-of]]
            [dataframe.series :as series]))


(expect '(0 1 2)
        (let [my-srs (srs/series '(:x :y :z))]
          (index my-srs)))

(expect 1
        (let [my-srs (srs/series '(1 2 3) '("A" "B" "C"))]
          (srs/ix my-srs "A")))

(expect nil
        (let [my-srs (srs/series '(1 2 3)'("A" "B" "C"))]
          (srs/ix my-srs "D")))

(expect "Bar"
        (let [my-srs (srs/series '(1 2 3)'("A" "B" "C"))]
          (srs/ix my-srs "D" "Bar")))


(expect AssertionError
        (let [my-srs (srs/series '(1 2 3) '("A" "B" "A"))]
          (srs/ix my-srs "D")))

; Test iteration
(expect '([:a 1] [:b 2] [:c 3])
        (map identity
             (srs/series [1 2 3] [:a :b :c])))


(expect (srs/series [2 4 6] [:a :b :c])
        (srs/mapvals
          (srs/series [1 2 3] [:a :b :c])
          #(* 2 %)))


(expect (srs/series [1 2 3] [:c :d :e])
        (srs/update-index
          (srs/series [1 2 3] [:a :b :c])
          [:c :d :e]))


(expect (srs/series [2 4] [:b :d])
        (srs/select
          (srs/series [1 2 3 4] [:a :b :c :d])
          [false true nil "true"]))



(expect (srs/series [false false true])
        (srs/gt
          (srs/series [1 5 10])
          5))


(expect (srs/series [116 120 125])
        (srs/add
          (srs/series [1 5 10])
          5
          10
          (srs/series [100 100 100])))


(expect (srs/series [6 nil 15])
        (srs/add
          (srs/series [1 nil 10])
          5))

(expect (srs/series [false true false])
        (series/eq (series/series [1 5 10]) 5))


(expect (srs/series [true false true])
        (series/neq (series/series [1 5 10]) 5))

(expect (more-of srs
                 (series/series [1] [0]) (series/subset srs 0 1)
                 (series/series [3 4] [2 3]) (series/subset srs 2 4)
                 (series/series [1 2] [0 1]) (series/head srs 2)
                 (series/series [6 7] [5 6]) (series/tail srs 2))
        (series/series [1 2 3 4 5 6 7]))


(expect 2
        (.valAt (series [1 2 3] [:a :b :c]) :b))

(expect true
        (contains? (series [1 2 3] [:a :b :c]) :b))

(expect false
        (contains? (series [1 2 3] [:a :b :c]) :d))


(expect [:b 2]
        (.entryAt (series [1 2 3] [:a :b :c]) :b))


(expect (series [1 2 3 4 5 6] [:a :b :c :d :e :f])
        (assoc
          (series [1 2 3 4 5] [:a :b :c :d :e])
          :f 6))

(expect (series [1 10 3 4 5] [:a :b :c :d :e])
        (assoc
          (series [1 2 3 4 5] [:a :b :c :d :e])
          :b 10))

(expect 2
        (get (series [1 2 3] [:a :b :c]) :b))


(expect '([:a 1] [:b 2] [:c 3])
        (seq (series/series [1 2 3] [:a :b :c])))


(expect '([:d 4] [:a 1] [:b 2] [:c 3])
        (cons [:d 4] (series/series [1 2 3] [:a :b :c])))


(expect true
        (= (series/series [1 2 3] [:a :b :c])
           (series/series [1 2 3] [:a :b :c])))

(expect false
        (= (series/series [1 2 3] [:a :b :d])
           (series/series [1 2 3] [:a :b :c])))

; Equality checks order
(expect false
        (= (series/series [3 2 1] [:c :b :a])
           (series/series [1 2 3] [:a :b :c])))


; Check that we iterate as pairs of index->val
(expect '([:a 1] [:b 2] [:c 3])
        (for [x  (series/series [1 2 3] [:a :b :c])]
          x))