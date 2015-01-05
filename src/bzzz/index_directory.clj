(ns bzzz.index-directory
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
  (use bzzz.index-directory)
  (use [clojure.string :only (join)])
  (:require [clojure.data.json :as json])
  (:require [bzzz.index-stat :as stat])
  (:require [bzzz.log :as log])
  (:require [clojure.java.io :as io])
  (:import (java.io StringReader File Writer FileNotFoundException)
           (java.lang OutOfMemoryError)
           (bzzz.java.store RedisDirectory)
           (redis.clients.jedis JedisPool)
           (org.apache.lucene.analysis Analyzer)
           (org.apache.lucene.facet.taxonomy.directory DirectoryTaxonomyWriter DirectoryTaxonomyReader)
           (org.apache.lucene.facet.taxonomy SearcherTaxonomyManager SearcherTaxonomyManager$SearcherAndTaxonomy)
           (org.apache.lucene.index IndexWriter IndexReader IndexWriterConfig DirectoryReader)
           (org.apache.lucene.search Query ScoreDoc SearcherManager IndexSearcher)
           (org.apache.lucene.store NIOFSDirectory Directory NoSuchDirectoryException)))

(declare use-searcher)
(declare use-writer)
(declare bootstrap-indexes)
(declare get-manager)
(declare try-close-manager)
(declare try-create-prefix)
(declare read-alias-file)
(declare refresh-single-searcher-manager)

(def root* (atom default-root))
(def alias* (atom {}))
(def redis* (atom {}))
(def identifier* (atom default-identifier))
(def write-refresh-lock* (atom {}))
(def name->manager* (atom {}))
(def unacceptable-name-pattern (re-pattern "[^a-zA-Z_0-9-:]"))
(def shard-suffix "-shard-")
(def shard-suffix-sre (str ".*" shard-suffix "\\d+"))

(defn index-dir-pattern
  ([] (re-pattern shard-suffix-sre))
  ([pre] (re-pattern (str "^" (as-str pre) shard-suffix-sre))))

(defn acceptable-index-name [name]
  (sanitize name unacceptable-name-pattern))

(defn root-identifier-path ^File []
  (io/file (as-str @root*) (as-str @identifier*)))

(defn sub-directories []
  (filter #(and (.isDirectory ^File %)
                (re-matches (index-dir-pattern)
                            (.getName ^File %)))
          (.listFiles (root-identifier-path))))

(defn alias-file ^File []
  (io/file (root-identifier-path) "alias.json"))

(defn initial-read-alias-file []
  (try
    (reset! alias* (json/read-str (slurp-or-default (alias-file) "{}")))
    (catch Exception e
      (do
        (if-not (instance? FileNotFoundException e)
          (log/warn (ex-str e)))
        (reset! alias* {})))))

(defn replace-alias-file []
  (try-create-prefix (root-identifier-path))
  (locking alias*
    (spit (alias-file) (json/write-str @alias*))))

(defn update-alias [index alias-del alias-set]
  (if alias-del
    (swap! alias* dissoc (as-str alias-del)))
  (if alias-set
    (swap! alias* assoc (as-str alias-set) (as-str index)))
  (replace-alias-file))

(defn resolve-alias [name]
  (if-let [r (get @alias* name)]
    (keyword r)
    name))

(defn index-name-matching [index]
  (map #(.getName ^File %)
       (filter #(not (nil? (re-matches (index-dir-pattern index) (.getName ^File %))))
               (sub-directories))))

(defn try-create-prefix [^File path-prefix]
  (try
    (.mkdir path-prefix)))

(defn try-redis [^File path]
  (locking redis*
    (if-let [pool (get @redis* path)]
      pool
      (let [f (if (.endsWith (.toString path) "_taxo__")
                (io/file (.getParentFile path) "redis.conf")
                (io/file path "redis.conf"))]
        (if (.exists f)
          (let [conf (jr (slurp f))
                pool (JedisPool. ^String (:host conf) (int-or-parse (:port conf)))]
            (swap! redis* assoc path pool)
            pool)
          nil)))))

(defn new-index-directory ^Directory [^File path-prefix name]
  (try-create-prefix path-prefix)
  (let [index-name (acceptable-index-name name)
        dir (io/file path-prefix index-name)
        redis (try-redis dir)]
    (if redis
      (RedisDirectory. index-name redis)
      (NIOFSDirectory. dir))))

(defn new-index-writer ^IndexWriter [name ^Analyzer analyzer]
  (IndexWriter. (new-index-directory (root-identifier-path) name)
                (IndexWriterConfig. *version* analyzer)))

(defn taxo-dir-prefix ^File [name]
  (io/file (root-identifier-path) (as-str name)))

(defn new-taxo-writer ^DirectoryTaxonomyWriter [name]
  (DirectoryTaxonomyWriter. (new-index-directory (taxo-dir-prefix name) (str "__" name "_taxo__"))))

(defn new-searcher-manager ^SearcherTaxonomyManager [name]
  (SearcherTaxonomyManager. (new-index-directory (root-identifier-path) name)
                            (new-index-directory (taxo-dir-prefix name) (str "__" name "_taxo__"))
                            nil))

(defn sharded [index x-shard]
  (let [shard (int-or-parse x-shard)]
    (str (as-str index) shard-suffix (str shard))))

(defn reset-search-managers []
  (doseq [[name manager] @name->manager*]
    (log/info "\tclosing: " name " " manager)
    (try-close-manager manager))
  (reset! name->manager* {}))

(defn use-searcher [index refresh callback]
  (let [^SearcherTaxonomyManager manager (get-manager index refresh)
        pair ^SearcherTaxonomyManager$SearcherAndTaxonomy (.acquire manager)
        t0 (time-ms)]
    (try
      (callback (.searcher pair) (.taxonomyReader pair))
      (catch Throwable e
        (do
          (stat/update-error index "use-searcher")
          (throw e)))
      (finally
        (do
          (.release manager pair)
          (stat/update-took-count index "use-searcher" (time-took t0)))))))

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

(defn try-close-manager [^SearcherTaxonomyManager manager]
  (try
    (.close manager)
    (catch Exception e
      (log/warn (as-str e)))))

(defn get-write-refresh-lock [index]
  (if-let [obj (get @write-refresh-lock* index)]
    obj
    (let [obj (Object.)]
      (swap! write-refresh-lock* assoc index obj)
      obj)))

(defmacro locking-took
  [x index stat-name & body]
  `(let [t0# (time-ms)]
     (locking ~x
       (stat/update-took-count ~index
                               (str ~stat-name "-to-lock")
                               (time-took t0#))
       (try
         ~@body
         (finally
           (stat/update-took-count ~index
                                   ~stat-name
                                   (time-took t0#)))))))

(defn use-writer [index analyzer force-merge callback]
  (locking-took (get-write-refresh-lock index)
                index
                "use-writer"
                (let [writer (new-index-writer index analyzer)
                      taxo (new-taxo-writer index)]
                  (try
                    (do
                      (.prepareCommit writer)
                      (.prepareCommit taxo)
                      (let [rc (callback ^IndexWriter writer ^DirectoryTaxonomyWriter taxo)
                            force-merge (int-or-parse force-merge)]
                        (let [t0 (time-ms)]
                          (.commit taxo)
                          (.commit writer)
                          (stat/update-took-count index "use-writer-commit" (time-took t0)))
                        (if (> force-merge 0)
                          (.forceMerge writer force-merge))
                        rc))
                    (catch Exception e
                      (do
                        (stat/update-error index "use-writer")
                        (if-not (instance? OutOfMemoryError e)
                          (do
                            (.rollback taxo)
                            (.rollback writer)))
                        (throw e)))
                    (finally
                      (do
                        (safe-close-taxo taxo)
                        (safe-close-writer writer)))))))


(defn use-writer-all [index callback]
  (doseq [name (index-name-matching index)]
    (use-writer name nil 1 callback)))

(defn bootstrap-indexes []
  (try
    (doseq [dir (sub-directories)]
      (try
        (get-manager (.getName ^File dir) false)
        (catch Exception e
          (log/warn (ex-str e)))))
    (catch Exception e
      (log/warn (ex-str e)))))

(defn get-manager [index refresh]
  (if-let [manager (get @name->manager* index)]
    (do
      (when refresh
        (refresh-single-searcher-manager index manager))
      manager)
    (locking name->manager*
      ;; in case someone created the manager just before we locked it
      (if-let [manager (get @name->manager* index)]
        manager
        ;; creating the searcher manager.
        ;;
        ;; to avoid a race just use the index name (including creating empty index)
        ;; at the very beginning, and also setup some basic stats for it
        (do
          (use-writer index nil 0 (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]))
          (let [created-manager (new-searcher-manager index)]
            (swap! name->manager* assoc index created-manager)
            (stat/get-statistics index)
            created-manager))))))

(defn refresh-single-searcher-manager [index ^SearcherTaxonomyManager manager]
  (log/debug "refreshing: " index " " manager)
  (try
    (locking-took (get-write-refresh-lock index)
                  index
                  "refresh-search-managers"
                  (.maybeRefresh manager))
    (catch Throwable e
      (do
        (log/info (str index " refresh exception, closing it. Exception: " (ex-str e)))
        (try-close-manager manager)
        (swap! name->manager* dissoc index)))))

(defn refresh-search-managers []
  (let [t0 (time-ms)]
    (bootstrap-indexes)
    (doseq [[index ^SearcherTaxonomyManager manager] @name->manager*]
      (refresh-single-searcher-manager index manager))
    (stat/update-took-count stat/total "refresh-search-managers" (time-took t0))
    (log/debug "refreshing took" (time-took t0))))

(defn shutdown []
  (locking name->manager*
    (log/info "executing shutdown hook, current mapping: " @name->manager*)
    (reset-search-managers)
    (log/info "mapping after cleanup: " @name->manager*)))

(defn index-stat []
  (into {} (for [[name manager] @name->manager*]
             [name (use-searcher name
                                 false
                                 (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                   (let [reader (.getIndexReader searcher)]
                                     {:docs (.numDocs reader)
                                      :searcher {:to-string (.toString searcher)
                                                 :sim (.toString (.getSimilarity searcher))}
                                      :stat (stat/get-statistics name)
                                      :manager {:to-string (.toString manager)}
                                      :reader {:to-string (.toString reader)
                                               :leaves (count (.leaves reader))
                                               :refcnt (.getRefCount reader)
                                               :deleted-docs (.numDeletedDocs reader)
                                               :has-deletions (.hasDeletions reader)}
                                      :taxo {:to-string (.toString taxo-reader)
                                             :size (.getSize taxo-reader)
                                             :refcnt (.getRefCount taxo-reader)}})))])))
