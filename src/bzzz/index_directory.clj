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
           (org.apache.lucene.facet.taxonomy.directory DirectoryTaxonomyWriter DirectoryTaxonomyReader)
           (org.apache.lucene.index IndexWriter IndexReader IndexWriterConfig DirectoryReader)
           (org.apache.lucene.search Query ScoreDoc SearcherManager IndexSearcher)
           (org.apache.lucene.store NIOFSDirectory Directory NoSuchDirectoryException)))

(declare use-searcher)
(declare use-writer)
(declare bootstrap-indexes)
(declare get-smanager-taxo)
(declare try-close-manager-taxo)

(def root* (atom default-root))
(def identifier* (atom default-identifier))
(def name->smanager-taxo* (atom {}))
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

(defn try-new-taxo-reader ^DirectoryTaxonomyReader [name]
  (try
    (new-taxo-reader name)
    (catch Exception e
      (if (instance? NoSuchDirectoryException e)
        (do
          (log/warn (ex-str e))
          (let [writer ^DirectoryTaxonomyWriter (new-taxo-writer name)]
            (.commit writer)
            (.close writer))
          (new-taxo-reader name))))))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))

(defn new-searcher-manager ^SearcherManager [name]
  (SearcherManager. (new-index-directory (root-identifier-path) name) nil))

(defn sharded [index x-shard]
  (let [shard (int-or-parse x-shard)]
    (str (as-str index) shard-suffix (str shard))))

(defn reset-search-managers []
  (doseq [[name [manager taxo]] @name->smanager-taxo*]
    (log/info "\tclosing: " name " " manager)
    (try-close-manager-taxo manager taxo))
  (reset! name->smanager-taxo* {}))

(defn use-searcher [index callback]
  (let [[^SearcherManager manager taxo] (get-smanager-taxo index)
        searcher ^IndexSearcher (.acquire manager)]
    (try
      (callback searcher taxo)
      (finally (.release manager searcher)))))

(defn log-close-err [^Directory dir ^Throwable e]
  (log/warn (str
             (.toString dir)
             " Got exception while close()ing the writer,and directory is still locked."
             " Unlocking it. "
             "[ should never happen, there is a race between this IndexWriter/unlock "
             " and other process/thread IndexWriter/open ]"
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

(defn try-close-manager-taxo [^SearcherManager manager ^DirectoryTaxonomyReader taxo]
  (try
    (do
      (try
        (.close taxo)
        (catch Exception e
          (log/warn (as-str e))))
      (.close manager))
    (catch Exception e
      (log/warn (as-str e)))))

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
        (get-smanager-taxo (.getName ^File dir))
        (catch Exception e
          (log/warn (ex-str e)))))
    (catch Exception e
      (log/warn (ex-str e)))))

(defn get-smanager-taxo [index]
  (locking name->smanager-taxo*
    (when (nil? (@name->smanager-taxo* index))
      (swap! name->smanager-taxo* assoc index [(new-searcher-manager index)
                                               (try-new-taxo-reader index)]))
    (@name->smanager-taxo* index)))

(defn refresh-search-managers []
  (let [start (time-ms)]
    (bootstrap-indexes)
    (locking name->smanager-taxo*
      (doseq [[index [^SearcherManager manager
                      ^DirectoryTaxonomyReader taxo]] @name->smanager-taxo*]
        (log/debug "refreshing: " index " " manager)
        (try
          (do
            (.maybeRefresh manager)
            (if-let [changed (DirectoryTaxonomyReader/openIfChanged taxo)]
              (swap! name->smanager-taxo* assoc-in [index 1] changed)))
          (catch Throwable e
            (do
              (log/info (str index " refresh exception, closing it. Exception: " (ex-str e)))
              (try-close-manager-taxo manager taxo)
              (swap! name->smanager-taxo* dissoc index))))))
    (log/debug "refreshing took" (time-took start))))

(defn shutdown []
  (locking name->smanager-taxo*
    (log/info "executing shutdown hook, current mapping: " @name->smanager-taxo*)
    (reset-search-managers)
    (log/info "mapping after cleanup: " @name->smanager-taxo*)))

(defn index-stat []
  (into {} (for [[name [manager taxo]] @name->smanager-taxo*]
             [name (use-searcher name
                                 (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                   (let [reader (.getIndexReader searcher)]
                                     {:docs (.numDocs reader)
                                      :searcher {:to-string (.toString searcher)
                                                 :sim (.toString (.getSimilarity searcher))}
                                      :manager {:to-string (.toString manager)}
                                      :reader {:to-string (.toString reader)
                                               :leaves (count (.leaves reader))
                                               :refcnt (.getRefCount reader)
                                               :deleted-docs (.numDeletedDocs reader)
                                               :has-deletions (.hasDeletions reader)}
                                      :taxo {:to-string (.toString taxo-reader)
                                             :size (.getSize taxo-reader)
                                             :refcnt (.getRefCount taxo-reader)}})))])))
