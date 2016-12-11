(ns dataframe.frame-test
  (:require [dataframe.frame :as frame :refer [index]]
            [expectations :refer [expect expect-focused more-of]]
            [dataframe.series :as series]
            [clojure.core :as core]))

; Constructors

(expect (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
  (frame/frame [[:x {:a 1 :b 2}]
                [:y {:a 2 :b 4}]
                [:z {:a 3 :b 6}]]))

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

(expect '([:x {:a 1 :b 2}]
           [:y {:a 2 :b 4}]
           [:z {:a 3 :b 6}])
  (-> (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
    frame/iterrows))

; Assert the iterator over a datafram
; iterates over [index row-map] pairs
(expect '([:x {:a 1 :b 2}]
           [:y {:a 2 :b 4}]
           [:z {:a 3 :b 6}])
  (for [x (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])]
    x))

(expect (frame/frame [[:x {:a 1 :b 2}]
                      [:y {:a 2 :b 4}]
                      [:z {:a 3 :b 6}]])
  (conj
    (frame/frame {:a '(1 2) :b '(2 4)} [:x :y])
    [:z {:a 3 :b 6}]))

(expect false
  (empty? (frame/frame {:a '(1 2) :b '(2 4)} [:x :y])))

(expect true
  (empty? (frame/frame {} [])))

(expect (series/series [3 6 9] [:x :y :z])
  (frame/maprows->srs
    (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
    (fn [row] (+ (:a row) (:b row)))))

(expect (frame/frame {:bar [-1 -2 -3] :foo [3 6 9]} [:x :y :z])
        (frame/maprows->df
          (frame/frame {:a '(1 2 3) :b '(2 4 6)} [:x :y :z])
          (fn [row] {:foo (+ (:a row) (:b row)) :bar (- (:a row) (:b row))})))

(expect (frame/frame {:a [1 2 3] :b [4 5 6]} [:x :y :z])
  (frame/frame
    [{:a 1 :b 4} {:a 2 :b 5} {:a 3 :b 6}]
    [:x :y :z]))

(expect (frame/frame [{:a 2 :b 6} {:a 4 :b 8}] [:x :z])
  (frame/select
    (frame/frame
      {:a [1 2 3 4] :b [5 6 7 8]}
      [:w :x :y :z])
    [false true nil "true"]))

;(expect (series/series [15])
;        (frame/with-context
;          (frame/frame [{:b 10}])
;          (series/add 5 $b)))


(expect '(+ 5 (core/get {:b 10} :b))
        (frame/replace-$-with-keys {:b 10} '(+ 5 $b) 'core/get))

(expect 15
        (eval (frame/replace-$-with-keys {:b 10} '(+ 5 $b) get)))

(expect 15
  (frame/with-> 12 (+ 5) (- 2)))

(expect (frame/frame {:a [1 2] :z [1 2]})
  (frame/with-> (frame/frame {:a [1 2]}) (frame/assoc-col :z $a)))

(expect 20
  (frame/with-> {:x {:y 20}} :x :y))

(expect (frame/frame [{:a 3 :b 300}] [2])

  (let [df (frame/frame {:a [1 2 3] :b [100 200 300]})]
    (frame/with-> df (frame/select (series/gt $a 2)))))

(expect (frame/frame {:a [1 2 3] :b [100 200 300] :c [10 20 30]})
  (let [df (frame/frame {:a [1 2] :b [100 200] :c [10 20]})]
    (frame/assoc-ix df 2 {:a 3 :b 300 :c 30})))

(expect (frame/frame {:a [1 2] :b [100 200] :c [10 20] :d [5 10]})
  (let [df (frame/frame {:a [1 2] :b [100 200] :c [10 20]})]
    (frame/assoc-col df :d [5 10])))

(expect (frame/frame {:a [1] :b [2]} [:x])
  (frame/head
    (frame/frame [[:x {:a 1 :b 2}]
                  [:y {:a 2 :b 4}]
                  [:z {:a 3 :b 6}]])
    1))

(expect 3
  (count (frame/frame {:a '(1 2 3) :b '(2 4 6)})))

(expect true
  (= (frame/frame {:a '(1 2 3) :b '(2 4 6)})
    (frame/frame {:a '(1 2 3) :b '(2 4 6)})))

(expect true
  (= (frame/frame {:b '(2 4 6) :a '(1 2 3)})
    (frame/frame {:a '(1 2 3) :b '(2 4 6)})))

(expect false
  (= (frame/frame {:a '(1 2 5) :b '(2 4 6)})
    (frame/frame {:a '(1 2 3) :b '(2 4 6)})))

(expect (frame/frame {:a [2 4 7] :b [4 2 8]} [:y :x :z])
  (frame/sort-rows (frame/frame [[:x {:a 4, :b 2}] [:y {:a 2, :b 4}] [:z {:a 7, :b 8}]])
    :a))

(expect (frame/frame {:a [1 2 3]
                      :b [10 20 30]
                      :c [1 2 3]
                      :d [10 20 30]})
        (frame/join
          (frame/frame {:a [1 2 3] :b [10 20 30]})
          (frame/frame {:c [1 2 3] :d [10 20 30]})
          :how :outer))

(expect (frame/frame {:a [1 2 3 nil nil]
                      :b [10 20 30 nil nil]
                      :c [nil nil 1 2 3]
                      :d [nil nil 10 20 30]})
        (frame/join
          (frame/frame {:a [1 2 3] :b [10 20 30]})
          (frame/frame {:c [1 2 3] :d [10 20 30]} [2 3 4])
          :how :outer))


(expect (frame/frame {:a-y [4 5 6]
                      :a-x [1 2 3]
                      :b [10 20 30]
                      :c [100 200 300]})
        (frame/join
          (frame/frame {:a [1 2 3] :b [10 20 30]})
          (frame/frame {:a [4 5 6] :c [100 200 300]})
          :how :outer))


(expect (frame/frame {:a-x [3]
                      :b [30]
                      :a-y [1]
                      :d [10]}
                     [2])
        (frame/join
          (frame/frame {:a [1 2 3] :b [10 20 30]})
          (frame/frame {:a [1 2 3] :d [10 20 30]} [2 3 4])
          :how :inner))

(expect (frame/frame {:a-x [1 2 3]
                      :b [10 20 30]
                      :a-y [nil nil 1]
                      :d [nil nil 10]}
                     [0 1 2])
        (frame/join
          (frame/frame {:a [1 2 3] :b [10 20 30]})
          (frame/frame {:a [1 2 3] :d [10 20 30]} [2 3 4])
          :how :left))


; Test handling of common columns

(expect "foobar" (frame/add-suffix "foo" "bar"))

(expect "foobar" (frame/add-suffix "foo" "bar"))

(expect "foobar" (frame/add-suffix "foo" :bar))

(expect :foobar (frame/add-suffix :foo :bar))

(expect :foobar (frame/add-suffix :foo "bar"))

(expect {:foo 10} (frame/assoc-common-column {} :foo 10 true #{:foo :bar} {:prefer-column :left}))

(expect {} (frame/assoc-common-column {} :foo 10 true #{:foo :bar} {:prefer-column :right}))

(expect {:baz 10} (frame/assoc-common-column {} :baz 10 true #{:foo :bar} {:prefer-column :right}))

(expect {:baz 10} (frame/assoc-common-column {} :baz 10 true #{:foo :bar} {:suffixes ["-left" "-right"]}))

(expect {:foo-left 10} (frame/assoc-common-column {} :foo 10 true #{:foo :bar} {:suffixes ["-left" "-right"]}))

(expect (frame/assoc-common-column {} :foo 10 false #{:foo :bar} {:suffixes ["-left" "-right"]}))


(expect (more-of grouped
                 (frame/frame {:a [1 1] :b [10 20]} [0 1]) (get grouped 1)
                 (frame/frame {:a [3] :b [30]} [2])        (get grouped 3))
        (frame/group-by-fn
          (frame/frame {:a [1 1 3] :b [10 20 30]}) :a))


(expect (more-of grouped
                 (frame/frame {:a [1 10] :b [10 1]} [0 2]) (get grouped 11)
                 (frame/frame {:a [1] :b [5]} [1])        (get grouped 6)
                 (frame/frame {:a [10] :b [17]} [3])        (get grouped 27))

        (frame/group-by-expr
          (frame/frame {:a [1 1 10 10] :b [10 5 1 17]}) (+ $a $b)))