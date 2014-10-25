(defproject bzzz "0.1.0-SNAPSHOT"
  :description "clojure + lucene + ring + jetty"
  :url "http://github.com/jackdoe/bzzz"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-marginalia "0.8.0"]]
  :jvm-opts ["-Xmx4g"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.lucene/lucene-core "4.9.1"]
                 [org.apache.lucene/lucene-facet "4.9.1"]
                 [org.apache.lucene/lucene-highlighter "4.9.1"]
                 [org.apache.lucene/lucene-queryparser "4.9.1"]
                 [org.apache.lucene/lucene-analyzers-common "4.9.1"]
                 [org.clojure/data.json "0.2.5"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.2.4"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]
                 [log4j/log4j "1.2.17"]
                 [overtone/at-at "1.2.0"]
                 [clj-http "1.0.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [ring/ring-core "1.3.1"]
                 [ring/ring-jetty-adapter "1.3.1"]]
  :main bzzz.core
  :aot [bzzz.core])
