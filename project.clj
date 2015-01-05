(defproject bzzz "0.1.0-SNAPSHOT"
  :description "clojure + lucene + ring + jetty"
  :url "http://github.com/jackdoe/bzzz"
  :maintainer {:email "jack@sofialondonmoskva.com"}
  :global-vars {*warn-on-reflection* true}
  :jvm-opts ["-Xmx2g"]
  :plugins [[lein-rpm "0.0.5"]] ;; .. used only because I couldnt find
                                ;; good way to add dependencies to
                                ;; '.lein-classpath' type tasks
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.googlecode.concurrentlinkedhashmap/concurrentlinkedhashmap-lru "1.4"]
                 [com.google.code.findbugs/jsr305 "3.0.0"]
                 [com.spatial4j/spatial4j "0.4.1"]
                 [org.apache.lucene/lucene-core "4.10.3"]
                 [org.apache.lucene/lucene-facet "4.10.3"]
                 [org.apache.lucene/lucene-spatial "4.10.3"]
                 [org.apache.lucene/lucene-highlighter "4.10.3"]
                 [org.apache.lucene/lucene-queryparser "4.10.3"]
                 [org.apache.lucene/lucene-analyzers-common "4.10.3"]
                 [org.apache.lucene/lucene-expressions "4.10.3"]
                 [org.clojure/data.json "0.2.5"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]
                 [log4j/log4j "1.2.17"]
                 [overtone/at-at "1.2.0"]
                 [com.googlecode.javaewah/JavaEWAH "0.9.2"]
                 [http-kit "2.1.16"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [redis.clients/jedis "2.6.0"] ;; drop the redis support?
                 [ring/ring-jetty-adapter "1.3.2"]
                 [ring/ring-core "1.3.2"]]
  :java-source-paths ["src/java"]
  :main bzzz.core
  :aot [bzzz.core])
