(ns dataframe.core
  (:require [dataframe.series]
            [dataframe.frame])
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
(def neq dataframe.series/neq)

; Imported frame methods

(def frame dataframe.frame/frame)

(def col dataframe.frame/col)

(def column-map dataframe.frame/column-map)

(def columns dataframe.frame/columns)

(def assoc-ix dataframe.frame/assoc-ix)

(def assoc-col dataframe.frame/assoc-col)

(def iterrows dataframe.frame/iterrows)

(def maprows->srs dataframe.frame/maprows->srs)
(def maprows->df dataframe.frame/maprows->df)

(def sort-rows dataframe.frame/sort-rows)

;(. (var with->) (setMacro))

;(defn with [& args]

(defmacro with-> [& args] `(dataframe.frame/with-> ~@args))

; Multi Methods


(defn delegate
  "Deligate the implementation of a multimethod to an existing function"
  [multifn dispatch-val f]
  (.. multifn (addMethod dispatch-val f)))

(defn first-type
  [& args]
  (type (first args)))

(defmulti ix first-type)
(delegate ix Series dataframe.series/ix)
(delegate ix Frame dataframe.frame/ix)

(defmulti index first-type)
(delegate index Series dataframe.series/index)
(delegate index Frame dataframe.frame/index)

(defmulti set-index first-type)
(delegate set-index Series dataframe.series/set-index)
(delegate set-index Frame dataframe.frame/set-index)

(defmulti select first-type)
(delegate select Series dataframe.series/select)
(delegate select Frame dataframe.frame/select)

(defmulti subset first-type)
(delegate subset Series dataframe.series/subset)
(delegate subset Frame dataframe.frame/subset)

(defmulti head first-type)
(delegate head Series dataframe.series/head)
(delegate head Frame dataframe.frame/head)

(defmulti tail first-type)
(delegate tail Series dataframe.series/tail)
(delegate tail Frame dataframe.frame/tail)