# dataframe

DataFrames for Clojure (inspired by Python's Pandas)


The dataframe package contains two core data structures:

- A Series is a map of index keys to values.  It is ordered and supports O(1) lookup of values by index key as well as O(1) lookup of values by position (based on the order).
- A Frame is a map of column names to Series, where each Series has an identical index.  A Frame may also be thought of as a map of index keys to maps, where each map is a row of a Frame that maps column names to the value in that row.



Series
======

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

The non-nil values of a series must all be of the same type.

DataFrame core has a number of functions for operating on or manipulating Series objects.

```clojure
(df/ix srs :b)
; 2

(df/values srs)
; [1 2 3]
```

One can apply arithmetic operations on a Series which return Series objects and which obey broadcast rules:

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


Frames
======

There are a number of equivalent ways to create a DataFrame.  These all use the `dataframe.core/frame` function.

The first is to pass a map of column names to column values as well as an optional index (if no index is passed, then a standard index of integers starting at 0 will be used).  The column values can either be sequences or they can be Series objects, but must all have the same length.


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

One can also construct a DataFrame from a list of pairs of index keys and rows-as-maps.

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

And, finally, one can construct a DataFrame from a list of maps and an optional index sequence:

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


DataFrame core has a number of functions for operating on or manipulating Series objects.

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

To make manipulating Frames easier, dataframe introduces the `with->` macro, which combines threading with easy access to the columns of a Frame.  This macro takes a Frame and threads it through a series of operations.  In doing so, when it encounters a symbol of the form `$col`, it knows to replace it with a reference to a column in the dataframe whose name is the keyword `:col` (for this reason, it is preferred to use keywords as column names).


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
 

DataFrame is distributed under the MIT license

Copyright © 2016 George Herbert Lewis

