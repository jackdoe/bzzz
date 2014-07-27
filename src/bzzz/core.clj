(ns bzzz.core
  (use ring.adapter.jetty)
  (:require [clojure.data.json :as json])
  (:import (java.io StringReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.core WhitespaceAnalyzer)
           (org.apache.lucene.document Document Field Field$Index Field$Store)
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery IndexSearcher Query ScoreDoc
                                     Scorer TermQuery)
           (org.apache.lucene.util Version AttributeSource)
           (org.apache.lucene.store NIOFSDirectory RAMDirectory Directory)))


(def ^{:dynamic true} *version* Version/LUCENE_CURRENT)
(def ^{:dynamic true} *analyzer* (WhitespaceAnalyzer. *version*))

(defn substring? [sub st]
 (not= (.indexOf st sub) -1))

(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
    (str x)))

(def root "/tmp/LUCY")

(defn new-index-writer ^IndexWriter [name]
  (IndexWriter. (NIOFSDirectory. (File. (File. (as-str root)) (as-str name)))
                (IndexWriterConfig. *version* *analyzer*)))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))

(defn- add-field [document key value]
  (let [ str-key (as-str key) ]
    (.add ^Document document
        (Field. str-key (as-str value)
                (if (substring? "_store" str-key)
                  Field$Store/YES
                  Field$Store/NO)
                (if (substring? "_index" str-key)
                  Field$Index/ANALYZED
                  Field$Index/NOT_ANALYZED)))))

(defn map->document
  [hmap]
  (let [document (Document.)]
    (doseq [[key value] hmap]
      (add-field document key value))
    document))

(defn document->map
  [^Document doc score]
  (into {:_score score } (for [^Field f (.getFields doc)]
                     [(keyword (.name f)) (.stringValue f)])))

(defn store
  [name maps]
  (let [writer (new-index-writer name)]
    (doseq [m maps]
      (.addDocument writer (map->document m)))
    (.commit writer)
    (.forceMerge writer 1)
    (.close writer)
    true))

(defn search
  [name query size]
  (with-open [reader (new-index-reader name)]
    (let [searcher (IndexSearcher. reader)
          parser (QueryParser. *version* "_default_" *analyzer*)
          query (.parse parser query)
          hits (.search searcher query (int size))
          m (into [] (for [hit (.scoreDocs hits)]
                                                (document->map (.doc ^IndexSearcher searcher (.doc ^ScoreDoc hit))
                                                               (.score ^ScoreDoc hit))))
          ]
      { :total (.totalHits hits), :hits m })))

(defn work [method input]
  (println method input)
  (if (= :post method)
    (store (:index input) (:documents input))
    (search (:index input) (:query input) (:size input))))

(defn handler [request]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/write-str (work (:request-method request) (json/read-str (slurp (:body request)) :key-fn keyword))) } )

(defn main []
  (run-jetty handler {:port 3000}))
