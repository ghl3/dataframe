(defproject dataframe "0.1.0-SNAPSHOT"

  :description "DataFrames in clojure"

  :url "http://example.com/FIXME"

  :license {:name "MIT License"
            :url "http://www.opensource.org/licenses/mit-license.php"}

  :dependencies [
                 [org.clojure/clojure "1.8.0"]
                 [net.mikera/core.matrix "0.54.0"]
                 [net.mikera/vectorz-clj "0.45.0"]

                 [org.apache.commons/commons-lang3 "3.0"]

                 [expectations "2.1.8"]

                 ]

  :source-paths ["src"]

  :java-source-paths ["java"]

  :plugins [[lein-expectations "0.0.7"]]

  )
