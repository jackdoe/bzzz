(defproject bzzz "0.1.0-SNAPSHOT"
  :description "clojure + lucene + ring + jetty"
  :url "http://github.com/jackdoe/i-have-no-idea-what-i-am-doing"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.lucene/lucene-core "4.9.0"]
                 [org.apache.lucene/lucene-queryparser "4.9.0"]
                 [org.apache.lucene/lucene-analyzers-common "4.9.0"]
                 [org.clojure/data.json "0.2.5"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.2.4"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]
                 [log4j/log4j "1.2.17"]
                 [overtone/at-at "1.2.0"]
                 [clj-http "1.0.0"]
                 [aleph "0.3.3"]
                 [org.clojure/core.async "0.1.338.0-5c5012-alpha"]
                 [ring/ring-core "1.3.0"]
                 [ring/ring-jetty-adapter "1.3.0"]]
  :main bzzz.core/main
  )
