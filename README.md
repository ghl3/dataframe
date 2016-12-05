# dataframe

DataFrames for Clojure (inspired by Python's Pandas)


The dataframe package contains two core data structures:

- A Series is a map of index keys to values.  It is ordered and supports O(1) lookup of values by index as well as O(1) lookup of values by positional offset (based on the order of the index).
- A Frame is a map of column names to column values, which are represented as Series, each with an identical index.  A Frame may also be thought of as a map of index keys to maps, where each map is a row of a Frame that maps column names to the value in that row.



Series
======

A series can be thought of as a 1-D vector of data with an index (vector of keys) for every value.  The keys are typically either integers or clojure Keywords, but can be any value.  Any of values may be nil, but the non-nil values must all be of the same type.

When iterated over, a Series is a collection of pairs of `[index value]`.  

| index | val |
|-------|-----|
| :a    | 10  |
| :b    | 20  |
| :c    | 30  |
| :d    | 40  |


To create a Series, pass a sequence of values and an index sequence to the constructor function:

```clojure

(require '[dataframe.core :as df])

(def srs (df/series [1 2 3] [:a :b :c]))
srs
```

<pre>
=> class dataframe.series.Series
:a 1
:b 2
:c 3
</pre>

DataFrame core has a number of functions for operating on or manipulating Series objects.

```clojure
(df/ix srs :b)
; 2

(df/values srs)
; [1 2 3]
```

One can apply arithmetic operations on a Series which return Series objects.  These operations obey broadcast rules: You may combine a primitive with a series which will apply the operation to every element of a series and return a new series with the same index as the first.  Or, you may apply a row-by-row operation on two series (if their indices exactly align):

```clojure
(df/add 1 srs)
```

<pre>
=> class dataframe.series.Series
:a 2
:b 3
:c 4
</pre>

```clojure
(df/eq 2 srs)
```

<pre>
=> class dataframe.series.Series
:a false
:b true
:c false
</pre>

```clojure
(df/add (series [1 2 3]) (series [10 20 30]))
```

<pre>
=> class dataframe.series.Series
0 11
1 22
2 33
</pre>


Frames
======

Frames are aligned collections of column-names to Series.  

When iterated over, a Frame is a collection of pairs of indexes to maps of rows: `[index {col->val}]`.  


| columns: | :a | :b | :c  |
|----------|----|----|-----|
| index    |    |    |     |
| :x       | 10 | 2  | 100 |
| :y       | 20 | 4  | 300 |
| :z       | 30 | 6  | 600 |



There are a number of equivalent ways to create a DataFrame.  These all use the `dataframe.core/frame` constructor function.  These ways are:

- Pass a map of column names to column values as well as an optional index (if no index is passed, then a standard index of integers starting at 0 will be used).  The column values can either be sequences or they can be Series objects, but must all have the same length.


```clojure

(require '[dataframe.core :as df])

(def frame (df/frame {:a [1 2 3] :b [10 20 30]} [:x :y :z]))
frame
```
<pre>
=> class dataframe.frame.Frame
	:a	:b
:x	1	10
:y	2	20
:z	3	30
</pre>

Here, `:a` and `:b` are the names of the columns and the index over rows is `[:x :y :z]`.

- Pass a list of pairs of index keys and rows-as-maps.

```clojure
(def frame (df/frame [[:x {:a 1 :b 10}]
                      [:y {:a 2 :b 20}]
                      [:z {:a 3 :b 30}]]))
frame
```
<pre>
=> class dataframe.frame.Frame
	:a	:b
:x	1	10
:y	2	20
:z	3	30
</pre>

- Pass a list of maps and an optional index sequence:

```clojure
(def frame (df/frame [{:a 1 :b 10}
                      {:a 2 :b 20}
                      {:a 3 :b 30}]
                      [:x :y :z]))
frame
```
<pre>
=> class dataframe.frame.Frame
	:a	:b
:x	1	10
:y	2	20
:z	3	30
</pre>


DataFrame core has a number of functions for operating on or manipulating Frames.

```clojure
(def frame (df/frame [[:x {:a 1 :b 10}]
                      [:y {:a 2 :b 20}]
                      [:z {:a 3 :b 30}]]))

(df/ix frame :x)
;=> class dataframe.series.Series
;:b 10
;:a 1

(df/col frame :a)
;=> class dataframe.series.Series
;:x 1
;:y 2
;:z 3


(df/assoc-col frame :c (df/add (df/col frame :a) (df/col frame :b)))
;=> class dataframe.frame.Frame
;	:b	:a	:c
;:x	10	1	11
;:y	20	2	22
;:z	30	3	33

```

To make manipulating Frames easier, dataframe introduces the `with->` macro, which combines Clojure's threading macro with notation for easily accessing the column of a Frame.  This macro takes a Frame and threads it through a series of operations.  In doing so, when it encounters a symbol of the form `$col`, it knows to replace it with a reference to a column in the dataframe whose name is the keyword `:col` (for this reason, it is preferred to use keywords as column names).


```clojure

(require '[dataframe.core :refer :all])

(def my-df (frame {:a [1 2 3] :b [10 20 30]}))

(with-> my-df
        (assoc-col :c (add $a 5))
        (assoc-col :d (add $b $c)))
```
<pre>
=> class dataframe.frame.Frame
	:a	:b	:c	:d
0	1	10	6	16
1	2	20	7	27
2	3	30	8	38
</pre>
 

Notice how the uses of `$a`, `$b`, and `$c` are replaced by the corresponding columns, as Series objects, in the dataframe pipeline above.  This allows us to leverage functions that act on Series objects to transform these columns and to use them to update the Frame object.

These pipelines can be arbitrarily complicated:

```clojure

(def my-df (frame [[:w {:a 0 :b 8}]
                   [:x {:a 1 :b 2}]
                   [:y {:a 2 :b 4}]
                   [:z {:a 3 :b 8}]]))
                   
(with-> my-df
        (select (and (lte $a 2) (gte $b 4)))
        (assoc-col :c (add $a $b))
        (map-rows->df (fn [row] {:foo (+ (:a row) (:c row))
                                 :bar (- (:b row) (:c row))}))
        (sort-rows :foo :bar)
        head)                  
```

<pre>
=> class dataframe.frame.Frame
	:bar	:foo
:y	-2		8
:w	0		8	
:z	-3		14
</pre>



DataFrame is distributed under the MIT license

Copyright © 2016 George Herbert Lewis

