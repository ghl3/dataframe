(ns dataframe.core
  (:refer-clojure :exclude [max])
  (:require [dataframe.series]
            [dataframe.frame]
            [clojure.core :as core])
  (:import (dataframe.series Series)
           (dataframe.frame Frame)))


(defn series [& args] (apply dataframe.series/series args))

(defn frame [& args] (apply dataframe.frame/frame args))


(defmulti ix class)
(defmethod ix Series [^Series srs & args] (apply dataframe.series/ix (concat [srs] args)))
(defmethod ix Frame [^Frame df & args] (apply dataframe.series/ix (concat [df] args)))

(defn col [^Frame df & args] (apply dataframe.frame/col (concat [df] args)))

(defn max [^Series srs] (apply core/max (:data srs)))