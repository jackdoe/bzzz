(ns bzzz.index
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
  (:require [clojure.tools.logging :as log])
  (:import (java.io StringReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.document Document Field Field$Index Field$Store)
           (org.apache.lucene.search.highlight Highlighter QueryScorer
                                               SimpleHTMLFormatter TextFragment)
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.search Query ScoreDoc SearcherManager IndexSearcher
                                     Explanation Collector TopScoreDocCollector TopDocsCollector)
           (org.apache.lucene.store NIOFSDirectory Directory)))
(set! *warn-on-reflection* true)
(def root* (atom default-root))
(def identifier* (atom default-identifier))
(def mapping* (atom {}))

(defn acceptable-index-name [name]
  (clojure.string/replace name #"[^a-zA-Z_0-9-]" ""))

(defn root-identifier-path []
  (File. ^File (File. (as-str @root*)) (as-str @identifier*)))

(defn new-index-directory ^Directory [name]
  (let [path ^File (root-identifier-path)]
    (.mkdir path)
    (NIOFSDirectory. (File. path (as-str (acceptable-index-name name))))))

(defn new-index-writer ^IndexWriter [name]
  (IndexWriter. (new-index-directory name)
                (IndexWriterConfig. *version* @analyzer*)))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))

(defn analyzed? [name]
  (if (or (substring? "_not_analyzed" name)
          (= name id-field))
    false
    true))

(defn norms? [name]
  (if (or (substring? "_no_norms" name)
          (= name id-field))
    false
    true))

(defn stored? [name]
  (if (and (substring? "_no_store" name)
           (not (= name id-field)))
    false
    true))

(defn indexed? [name]
  (if (and (substring? "_no_index" name)
           (not (= name id-field)))
    false
    true))

(defn- add-field [document key value]
  (let [ str-key (as-str key) ]
    (.add ^Document document
          (Field. str-key (as-str value)
                  (if (stored? str-key)
                    Field$Store/YES
                    Field$Store/NO)
                  (if (indexed? str-key)
                    (case [(analyzed? str-key) (norms? str-key)]
                      [true true] Field$Index/ANALYZED
                      [true false] Field$Index/ANALYZED_NO_NORMS
                      [false true] Field$Index/NOT_ANALYZED
                      [false false] Field$Index/NOT_ANALYZED_NO_NORMS)
                    Field$Index/NO)))))

(defn map->document
  [hmap]
  (let [document (Document.)]
    (doseq [[key value] hmap]
      (add-field document (as-str key) value))
    document))

(defn document->map
  [^Document doc score highlighter ^Explanation explanation]
  (let [m (into {:_score score }
                (for [^Field f (.getFields doc)]
                  [(keyword (.name f)) (.stringValue f)]))
        fragments (highlighter m)]
    (conj
     m
     (when explanation (assoc m :_explain (.toString explanation)))
     (when fragments   (assoc m :_highlight fragments)))))

(defn get-search-manager ^SearcherManager [index]
  (locking mapping*
    (when (nil? (@mapping* index))
      (swap! mapping* assoc index (SearcherManager. (new-index-directory index)
                                                    nil)))
    (@mapping* index)))

(defn refresh-search-managers []
  (locking mapping*
    (doseq [[index ^SearcherManager manager] @mapping*]
      (log/debug "refreshing: " index " " manager)
      (.maybeRefresh manager))))

(defn reset-search-managers []
  (doseq [[name ^SearcherManager manager] @mapping*]
    (log/info "\tclosing: " name " " manager)
    (.close manager))
  (reset! mapping* {}))

(defn bootstrap-indexes []
  (doseq [f (.listFiles (File. (as-str @root*)))]
    (if (.isDirectory ^File f)
      (get-search-manager (.getName ^File f)))))

(defn use-searcher [index callback]
  (let [manager (get-search-manager index)
        searcher (.acquire manager)]
    (try
      (callback searcher)
      (finally (.release manager searcher)))))

(defn use-writer [index callback]
  (let [writer (new-index-writer index)]
    (try
      (callback ^IndexWriter writer)
      (finally
        (.commit writer)
        (.forceMerge writer 1)
        (.close writer)))))

(defn store
  [index maps analyzer]
  (if analyzer
    (reset! analyzer* (parse-analyzer analyzer)))
  (use-writer index (fn [^IndexWriter writer]
                      (doseq [m maps]
                        (if (:id m)
                          (.updateDocument writer ^Term (Term. ^String id-field
                                                               (as-str (:id m))) (map->document m))
                          (.addDocument writer (map->document m))))
                      { index true })))

(defn delete-from-query
  [index input]
  (use-writer index (fn [^IndexWriter writer]
                      ;; method is deleteDocuments(Query...)
                      (let [query (parse-query input (extract-analyzer nil))]
                        (.deleteDocuments writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query [query]))
                        { index (.toString query) }))))

(defn delete-all [index]
  (use-writer index (fn [^IndexWriter writer] (.deleteAll writer))))


(defn fragment->map [^TextFragment fragment]
  {:text (.toString fragment)
   :score (.getScore fragment)
   :frag-num (wall-hack-field TextFragment :fragNum fragment)
   :text-start-pos (wall-hack-field TextFragment :textStartPos fragment)
   :text-end-pos (wall-hack-field TextFragment :textEndPos fragment)})

(defn- make-highlighter
  [^Query query ^IndexSearcher searcher config analyzer]
  (if config
    (let [indexReader (.getIndexReader searcher)
          scorer (QueryScorer. (.rewrite query indexReader))
          config (merge {:field :_content
                         :max-fragments 5
                         :use-text-fragments false
                         :separator "..."
                         :pre "<b>"
                         :post "</b>"}
                        config)
          {:keys [field max-fragments separator fragments-key pre post use-text-fragments]} config
          highlighter (Highlighter. (SimpleHTMLFormatter. pre post) scorer)]
      (fn [m]
        (let [str (need (keyword field) m "highlight field not found in doc")
              token-stream (.tokenStream ^Analyzer analyzer
                                         (as-str field)
                                         (StringReader. str))]
          (if use-text-fragments
            (map #(fragment->map %)
                 (.getBestTextFragments ^Highlighter highlighter
                                        ^TokenStream token-stream
                                        ^String str
                                        true
                                        (int max-fragments)))
            (.getBestFragments ^Highlighter highlighter
                               ^TokenStream token-stream
                               ^String str
                               (int max-fragments)
                               ^String separator)))))
    (constantly nil)))

(defn search
  [& {:keys [index query page size explain refresh highlight analyzer]
      :or {page 0, size 20, explain false refresh false analyzer nil}}]
  (if refresh
    (refresh-search-managers))
  (use-searcher index
                (fn [^IndexSearcher searcher]
                  (let [ms-start (time-ms)
                        analyzer (extract-analyzer analyzer)
                        query (parse-query query analyzer)
                        highlighter (make-highlighter query searcher highlight analyzer)
                        pq-size (+ (* page size) size)
                        collector ^TopDocsCollector (TopScoreDocCollector/create pq-size true)]
                    (.search searcher query collector)
                    {:total (.getTotalHits collector)
                     :hits (into []
                                 (for [^ScoreDoc hit (-> (.topDocs collector (* page size)) (.scoreDocs))]
                                   (document->map (.doc searcher (.doc hit))
                                                  (.score hit)
                                                  highlighter
                                                  (when explain
                                                    (.explain searcher query (.doc hit))))))
                     :took (time-took ms-start)}))))

(defn shutdown []
  (locking mapping*
    (log/info "executing shutdown hook, current mapping: " @mapping*)
    (reset-search-managers)
    (log/info "mapping after cleanup: " @mapping*)))

(defn index-stat []
  (into {} (for [[name searcher] @mapping*]
             [name (use-searcher name
                                 (fn [^IndexSearcher searcher]
                                   (let [reader (.getIndexReader searcher)]
                                     {:docs (.numDocs reader)
                                      :has-deletions (.hasDeletions reader)})))])))
