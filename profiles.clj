{:dev {:dependencies [[expectations "2.1.9"]]

       :plugins [[jonase/eastwood "0.2.3"]
                 [lein-cljfmt "0.5.6" :exclusions [org.clojure/clojure]]
                 [lein-expectations "0.0.8" :exclusions [org.clojure/clojure]]]

       ; Generate docs
       :codox {:output-path "resources/codox"
               :metadata {:doc/format :markdown}
               :source-uri "http://github.com/lendup/citadel/blob/master/{filepath}#L{line}"}

       :eastwood {:exclude-namespaces [:test-paths]}

       ; Format code
       :cljfmt {:indents
                {require [[:block 0]]
                 ns [[:block 0]]
                 #"^(?!:require|:import).*" [[:inner 0]]}}}
}
