(ns dataframe.util)


(defn zip
  "Take a number of iterables and
  return a single iterable over vectors,
  each containing the ith element of each
  input iterable (ordered by the order of
  the input iterables).
  If the input iterables are not of the same
  length, the returned iterable is as long as
  the shortest input iterable (data from
  longer input interables will not be
  returned).
  "
  [& args]
  (apply map vector args))


(defn enumerate
  ([xs] (enumerate xs true))
  ([xs index-first?]
   (if index-first?
     (zip (range) xs)
     (zip xs (range)))))