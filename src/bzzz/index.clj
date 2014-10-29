(ns bzzz.index
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
  (use bzzz.random-score-query)
  (use [clojure.string :only (join)])
  (:require [clojure.tools.logging :as log])
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
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.search Query ScoreDoc SearcherManager IndexSearcher
                                     Explanation Collector TopScoreDocCollector
                                     TopDocsCollector MultiCollector)
           (org.apache.lucene.store NIOFSDirectory Directory)))

(def root* (atom default-root))
(def identifier* (atom default-identifier))
(def mapping* (atom {}))

(defn acceptable-index-name [name]
  (clojure.string/replace name #"[^a-zA-Z_0-9-]" ""))

(defn root-identifier-path ^File []
  (File. ^File (File. (as-str @root*)) (as-str @identifier*)))

(defn new-index-directory ^Directory [name]
  (let [path ^File (root-identifier-path)]
    (try
      (.mkdir path))
    (NIOFSDirectory. (File. path (as-str (acceptable-index-name name))))))

(defn new-index-writer ^IndexWriter [name]
  (IndexWriter. (new-index-directory name)
                (IndexWriterConfig. *version* @analyzer*)))

(def taxo-prefix "__taxo-")

(defn taxo-dir-name [name]
  (str taxo-prefix (as-str name)))

(defn new-taxo-writer ^DirectoryTaxonomyWriter [name]
  (DirectoryTaxonomyWriter. (new-index-directory (taxo-dir-name name))))

(defn new-taxo-reader ^DirectoryTaxonomyReader [name]
  (DirectoryTaxonomyReader. (new-index-directory (taxo-dir-name name))))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))


(defn text-field [^Field$Store stored ^String key value]
  (Field. key (as-str value)
          stored
          (if (indexed? key)
            (case [(analyzed? key) (norms? key)]
              [true true] Field$Index/ANALYZED
              [true false] Field$Index/ANALYZED_NO_NORMS
              [false true] Field$Index/NOT_ANALYZED
              [false false] Field$Index/NOT_ANALYZED_NO_NORMS)
            Field$Index/NO)))

(defn numeric-field [^Field$Store stored ^String key ^String value]
  (if (index_integer? key)
    (IntField. key (Integer/parseInt value) stored)
    (if (index_long? key)
      (LongField. key (Long/parseLong value) stored)
      (if (index_float? key)
        (FloatField. key (Float/parseFloat value) stored)
        (DoubleField. key (Double/parseDouble value) stored)))))

(defn add-field [document key value]
  (let [str-key (as-str key)
        stored (if (stored? str-key)
                 Field$Store/YES
                 Field$Store/NO)
        field (if (numeric? str-key)
                (numeric-field stored str-key value)
                (text-field stored str-key value))]
    (.add ^Document document field)))

(defn map->document [hmap]
  (let [document (Document.)]
    (doseq [[key value] hmap]
      (add-field document (as-str key) value))
    document))

(defn document->map
  [^Document doc score highlighter ^Explanation explanation]
  (let [m (into {:_score score }
                (for [^Field f (.getFields doc)]
                  [(keyword (.name f)) (.stringValue f)]))
        highlighted (highlighter m)]
    (conj
     m
     (when explanation (assoc m :_explain (.toString explanation)))
     (when highlighted (assoc m :_highlight highlighted)))))

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
      ;; FIXME - reopen cached taxo readers
      (.maybeRefresh manager))))

(defn reset-search-managers []
  (doseq [[name ^SearcherManager manager] @mapping*]
    (log/info "\tclosing: " name " " manager)
    (.close manager))
  (reset! mapping* {}))

(defn use-taxo-reader [index callback]
  ;; FIXME: reuse
  (let [reader (new-taxo-reader index)]
    (try
      (callback reader)
      (finally (.close reader)))))

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
             "[ should never happen, there is a race between this IndexWriter/unlock and other process/thread index writer open ]"
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

(defn bootstrap-indexes []
  (doseq [f (filter (fn [x] (= (.indexOf (.getName ^File x) (as-str taxo-prefix)) -1))
                    (.listFiles (root-identifier-path)))]
    (if (.isDirectory ^File f)
      (let [name (.getName ^File f)]
        (use-writer name (fn [writer taxo]
                           ;; do nothing, just open/close the taxo directory
                           ))
        (get-search-manager name)))))

(defn get-facet-config ^FacetsConfig [facets]
  (let [config (FacetsConfig.)]
    (doseq [[dim f-info] facets]
      (.setMultiValued config (as-str dim) true)
      (.setRequireDimCount config (as-str dim) true))
    config))

(defn add-facet-field-single [^Document doc dim val]
  (.add ^Document doc
        (FacetField. (as-str dim)
                     ^"[Ljava.lang.String;" (into-array String [val]))))

(defn add-facet-field [^Document doc dim val info ^Analyzer analyzer]
  (if (:use-analyzer info)
    (let [stream (.tokenStream analyzer (as-str (:use-analyzer info)) (StringReader. val))
          termAtt (.getAttribute stream CharTermAttribute)]
      (.reset stream)
      (while (.incrementToken stream)
        (add-facet-field-single doc dim (.toString termAtt)))
      (.close stream))
    (add-facet-field-single doc dim val)))

(defn store
  [& {:keys [index documents analyzer facets]
      :or {analyzer nil facets {}}}]
  (if analyzer
    (reset! analyzer* (parse-analyzer analyzer)))
  (use-writer index (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]
                      (let [config (get-facet-config facets)]
                        (doseq [m documents]
                          (let [doc (map->document m)]
                            (doseq [[dim f-info] facets]
                              (if-let [f-val ((keyword dim) m)]
                                (add-facet-field doc dim f-val f-info @analyzer*)))
                            (if (:id m)
                              (.updateDocument writer ^Term (Term. ^String id-field
                                                                   (as-str (:id m))) (.build config doc))
                              (.addDocument writer (.build config taxo doc))))))
                      { index true })))

(defn delete-from-query
  [index input]
  (use-writer index (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]
                      ;; method is deleteDocuments(Query...)
                      (let [query (parse-query input (extract-analyzer nil))]
                        (.deleteDocuments writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query [query]))
                        { index (.toString query) }))))

(defn delete-all [index]
  (use-writer index (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo] (.deleteAll writer))))

(defn fragment->map [^TextFragment fragment]
  {:text (.toString fragment)
   :score (.getScore fragment)
   :frag-num (wall-hack-field (class fragment) :fragNum fragment)
   :text-start-pos (wall-hack-field (class fragment) :textStartPos fragment)
   :text-end-pos (wall-hack-field (class fragment) :textEndPos fragment)})

(defn- make-highlighter
  [^Query query ^IndexSearcher searcher config analyzer]
  (if config
    (let [indexReader (.getIndexReader searcher)
          scorer (QueryScorer. (.rewrite query indexReader))
          config (merge {:max-fragments 5
                         :pre "<b>"
                         :post "</b>"}
                        config)
          {:keys [fields max-fragments separator fragments-key pre post use-text-fragments]} config
          highlighter (Highlighter. (SimpleHTMLFormatter. pre post) scorer)]
      (fn [m]
        (into {} (for [field fields]
                   [(keyword field)
                    (if-let [str ((keyword field) m)]
                      (map #(fragment->map %)
                           (.getBestTextFragments ^Highlighter highlighter
                                                  ^TokenStream (.tokenStream ^Analyzer analyzer
                                                                             (as-str field)
                                                                             (StringReader. str))
                                                  ^String str
                                                  true
                                                  (int max-fragments)))
                      [])]))))
    (constantly nil)))

(defn search
  [& {:keys [index query page size explain refresh highlight analyzer facets]
      :or {page 0, size default-size, explain false refresh false analyzer nil facets nil}}]
  (if refresh
    (refresh-search-managers))
  (use-searcher index
                (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                  (let [ms-start (time-ms)
                        analyzer (extract-analyzer analyzer)
                        query (parse-query query analyzer)
                        highlighter (make-highlighter query searcher highlight analyzer)
                        pq-size (+ (* page size) size)
                        score-collector ^TopDocsCollector (TopScoreDocCollector/create pq-size true)
                        facet-collector (FacetsCollector.)
                        facet-config (get-facet-config facets)
                        wrap (MultiCollector/wrap
                              ^"[Lorg.apache.lucene.search.Collector;" (into-array Collector
                                                                                   [score-collector
                                                                                    facet-collector]))]
                    (.search searcher query wrap)
                    {:total (.getTotalHits score-collector)
                     :facets (try (let [fc (FastTaxonomyFacetCounts. taxo-reader
                                                                     facet-config
                                                                     facet-collector)]
                                    (into {} (for [[k v] facets]
                                               (if-let [fr (.getTopChildren fc
                                                                            (default-to (:size v) default-size)
                                                                            (as-str k)
                                                                            ^"[Ljava.lang.String;" (into-array
                                                                                                    String []))]
                                                 [(keyword (.dim fr))
                                                  (into [] (for [^LabelAndValue lv (.labelValues fr)]
                                                             {:label (.label lv)
                                                              :count (.value lv)}))]))))
                                  (catch Throwable e
                                    (let [ex (ex-str e)]
                                      (log/warn (ex-str e))
                                      {}))) ;; do not send the error back,
                                            ;; even though we might fake a facet result
                                            ;; it could really surprise the client
                     :hits (into []
                                 (for [^ScoreDoc hit (-> (.topDocs score-collector (* page size)) (.scoreDocs))]
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
             (let [sample (search :index name
                                  :query {:random-score {:query {:match-all {}}}})]
               [name (use-searcher name
                                   (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                     (let [reader (.getIndexReader searcher)]
                                       {:docs (.numDocs reader)
                                        :sample sample
                                        :has-deletions (.hasDeletions reader)})))]))))
