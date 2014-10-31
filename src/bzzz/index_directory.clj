(ns bzzz.index-directory
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
  (use bzzz.index-directory)
  (use bzzz.random-score-query)
  (use [clojure.string :only (join)])
  (:require [clojure.tools.logging :as log])
  (:require [clojure.data.json :as json])
  (:require [clojure.java.io :as io])
  (:import (java.io StringReader File)
           (java.lang OutOfMemoryError)
           (org.apache.lucene.analysis.tokenattributes CharTermAttribute)
           (org.apache.lucene.facet FacetsConfig FacetField FacetsCollector LabelAndValue)
           (org.apache.lucene.facet.taxonomy FastTaxonomyFacetCounts)
           (org.apache.lucene.facet.taxonomy.directory DirectoryTaxonomyWriter DirectoryTaxonomyReader)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.document Document Field Field$Index Field$Store
                                       IntField LongField FloatField DoubleField)
           (org.apache.lucene.search.highlight Highlighter QueryScorer
                                               SimpleHTMLFormatter TextFragment)
           (org.apache.lucene.index IndexWriter IndexReader Term IndexableField
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.search Query ScoreDoc SearcherManager IndexSearcher
                                     Explanation Collector TopScoreDocCollector
                                     TopDocsCollector MultiCollector FieldValueFilter)
           (org.apache.lucene.store NIOFSDirectory Directory)))

(declare use-searcher)
(declare use-writer)
(declare bootstrap-indexes)
(declare get-search-manager)

(def root* (atom default-root))
(def identifier* (atom default-identifier))
(def mapping* (atom {}))

(def acceptable-name-pattern (re-pattern "[^a-zA-Z_0-9-]"))
(def shard-suffix "-shard-")
(def shard-suffix-sre (str ".*" shard-suffix "\\d+"))

(defn index-dir-pattern
  ([] (re-pattern shard-suffix-sre))
  ([pre] (re-pattern (str "^" (as-str pre) shard-suffix-sre))))

(defn acceptable-index-name [name]
  (clojure.string/replace name acceptable-name-pattern ""))

(defn root-identifier-path ^File []
  (io/file (as-str @root*) (as-str @identifier*)))

(defn sub-directories []
  (filter #(and (.isDirectory ^File %)
                (re-matches (index-dir-pattern)
                            (.getName ^File %)))
          (.listFiles (root-identifier-path))))

(defn index-name-matching [index]
  (map #(.getName ^File %)
       (filter #(not (nil? (re-matches (index-dir-pattern index) (.getName ^File %))))
               (sub-directories))))

(defn new-index-directory ^Directory [^File path-prefix name]
  (try
    (.mkdir path-prefix))
  (NIOFSDirectory. (io/file path-prefix (as-str (acceptable-index-name name)))))

(defn new-index-writer ^IndexWriter [name]
  (IndexWriter. (new-index-directory (root-identifier-path) name)
                (IndexWriterConfig. *version* @analyzer*)))

(defn taxo-dir-prefix ^File [name]
  (io/file (root-identifier-path) (as-str name)))

(defn new-taxo-writer ^DirectoryTaxonomyWriter [name]
  (DirectoryTaxonomyWriter. (new-index-directory (taxo-dir-prefix name) "__taxo__")))

(defn new-taxo-reader ^DirectoryTaxonomyReader [name]
  (DirectoryTaxonomyReader. (new-index-directory (taxo-dir-prefix name) "__taxo__")))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))

(defn sharded [index x-shard]
  (let [shard (int-or-parse x-shard)]
    (str (as-str index) shard-suffix (str shard))))

(defn reset-search-managers []
  (doseq [[name ^SearcherManager manager] @mapping*]
    (log/info "\tclosing: " name " " manager)
    (.close manager))
  (reset! mapping* {}))

(defn use-taxo-reader [index callback]
  ;; FIXME: reuse
  (let [reader (try
                 (new-taxo-reader index)
                 (catch Throwable e
                   (do
                     (log/warn (ex-str e))
                     nil)))]
    (try
      (callback reader)
      (finally (if reader
                 (.close ^DirectoryTaxonomyReader reader))))))

(defn use-searcher [index callback]
  (let [manager (get-search-manager index)
        searcher (.acquire manager)]
    (try
      (use-taxo-reader index (fn [^DirectoryTaxonomyReader reader] (callback searcher reader)))
      (finally (.release manager searcher)))))

(defn log-close-err [^Directory dir ^Throwable e]
  (log/warn (str
             (.toString dir)
             " Got exception while close()ing the writer,and directory is still locked."
             " Unlocking it. "
             "[ should never happen, there is a race between this IndexWriter/unlock and other process/thread IndexWriter/open ]"
             " Exception: " (ex-str e))))

(defn safe-close-writer [^IndexWriter writer]
  (try
    (do
      (.close writer))
    (catch Throwable e
      (if (IndexWriter/isLocked (.getDirectory writer))
        (do
          (log-close-err (.getDirectory writer) e)
          (IndexWriter/unlock (.getDirectory writer)))))))

(defn safe-close-taxo [^DirectoryTaxonomyWriter taxo]
  (try
    (.close taxo)
    (catch Throwable e
      (if (IndexWriter/isLocked (.getDirectory taxo))
        (do
          (log-close-err (.getDirectory taxo) e)
          (DirectoryTaxonomyWriter/unlock (.getDirectory taxo)))))))

(defn use-writer [index callback]
  (let [writer (new-index-writer index)
        taxo (new-taxo-writer index)]
    (try
      (do
        (.prepareCommit writer)
        (.prepareCommit taxo)
        (let [rc (callback ^IndexWriter writer ^DirectoryTaxonomyWriter taxo)]
          (.commit taxo)
          (.commit writer)
          (.forceMerge writer 1)
          rc))
      (catch Exception e
        (do
          (if-not (instance? OutOfMemoryError e)
            (do
              (.rollback taxo)
              (.rollback writer)))
          (throw e)))
      (finally
        (do
          (safe-close-taxo taxo)
          (safe-close-writer writer))))))

(defn use-writer-all [index callback]
  (doseq [name (index-name-matching index)]
    (use-writer name callback)))

(defn bootstrap-indexes []
  (try
    (doseq [dir (sub-directories)]
      (try
        (get-search-manager (.getName dir))
        (catch Exception e
          (log/warn (ex-str e)))))
    (catch Exception e
      (log/warn (ex-str e)))))

(defn get-search-manager ^SearcherManager [index]
  (locking mapping*
    (when (nil? (@mapping* index))
      (swap! mapping* assoc index (SearcherManager. (new-index-directory (root-identifier-path) index)
                                                    nil)))
    (@mapping* index)))

(defn refresh-search-managers []
  (bootstrap-indexes)
  (locking mapping*
    (doseq [[index ^SearcherManager manager] @mapping*]
      (log/debug "refreshing: " index " " manager)
      ;; FIXME - reopen cached taxo readers
      (try
        (.maybeRefresh manager)
        (catch Throwable e
          (do
            (log/info (str index " maybeRefresh exception, closing it. Exception: " (ex-str e)))
            (.close manager)
            (swap! mapping* dissoc index)))))))

(defn shutdown []
  (locking mapping*
    (log/info "executing shutdown hook, current mapping: " @mapping*)
    (reset-search-managers)
    (log/info "mapping after cleanup: " @mapping*)))

(defn index-stat []
  (into {} (for [[name searcher] @mapping*]
             [name (use-searcher name
                                 (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                   (let [reader (.getIndexReader searcher)]
                                     {:docs (.numDocs reader)
                                      :has-deletions (.hasDeletions reader)})))])))
