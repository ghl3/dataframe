(ns dataframe.core
  (:refer-clojure :exclude [max])
  (:import (dataframe.series Series)
           (dataframe.frame Frame)))


; Imported series methods

(def series dataframe.series/series)
(def series? dataframe.series/series?)
(def values dataframe.series/values)
(def update-key dataframe.series/update-key)
(def mapvals dataframe.series/mapvals)

(def lt dataframe.series/lt)
(def lte dataframe.series/lte)
(def gt dataframe.series/gt)
(def gte dataframe.series/gte)
(def add dataframe.series/add)
(def sub dataframe.series/sub)
(def mul dataframe.series/mul)
(def div dataframe.series/div)
(def eq dataframe.series/eq)
(def new dataframe.series/neq)


; Imported frame methods

(def frame dataframe.frame/frame)

(def col dataframe.frame/col)

(def column-map dataframe.frame/column-map)

(def columns dataframe.frame/columns)

(def assoc-ix dataframe.frame/assoc-ix)

(def assoc-col dataframe.frame/assoc-col)

(def iterrows dataframe.frame/iterrows)

(def map-rows->srs dataframe.frame/map-rows->srs)
(def map-rows->df dataframe.frame/map-rows->df)


;(. (var with->) (setMacro))

;(defn with [& args]

(defmacro with-> [& args] `(dataframe.frame/with-> ~@args))


; Multi Methods


(defn deligate
  "Deligate the implementation of a multimethod to an existing function"
  [multifn dispatch-val f]
  (.. multifn (addMethod dispatch-val f)))

(defn first-class
  [& args]
  (class (first args)))


(defmulti ix first-class)
(deligate ix Series dataframe.series/ix)
(deligate ix Frame dataframe.frame/ix)

(defmulti index first-class)
(deligate index Series dataframe.series/index)
(deligate index Frame dataframe.frame/index)

(defmulti set-index first-class)
(deligate set-index Series dataframe.series/set-index)
(deligate set-index Frame dataframe.frame/set-index)

(defmulti select first-class)
(deligate select Series dataframe.series/select)
(deligate select Frame dataframe.frame/select)

(defmulti subset first-class)
(deligate subset Series dataframe.series/subset)
(deligate subset Frame dataframe.frame/subset)

(defmulti head first-class)
(deligate head Series dataframe.series/head)
(deligate head Frame dataframe.frame/head)

(defmulti tail first-class)
(deligate tail Series dataframe.series/tail)
(deligate tail Frame dataframe.frame/tail)