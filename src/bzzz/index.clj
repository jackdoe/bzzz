(ns bzzz.index
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
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

(def root* (atom default-root))
(def identifier* (atom default-identifier))
(def mapping* (atom {}))

(defn acceptable-index-name [name]
  (clojure.string/replace name #"[^a-zA-Z_0-9-]" ""))

(defn root-identifier-path ^File []
  (io/file (as-str @root*) (as-str @identifier*)))

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
    (IntField. key (int-or-parse value) stored)
    (if (index_long? key)
      (LongField. key (long-or-parse value) stored)
      (if (index_float? key)
        (FloatField. key (float-or-parse value) stored)
        (DoubleField. key (double-or-parse value) stored)))))

(defn add-field [^Document document key value]
  (let [str-key (as-str key)
        stored (if (stored? str-key)
                 Field$Store/YES
                 Field$Store/NO)
        generator (if (numeric? str-key)
                    #(numeric-field stored str-key %)
                    #(text-field stored str-key %))]
    (if (vector? value)
      (do
        (if (stored? str-key)
          (.add document (Field. str-key
                                 "__array_identifier__"
                                 Field$Store/YES
                                 Field$Index/NO)))
        (doseq [v value]
          (.add document (generator v))))
      (.add document (generator value)))))

(defn map->document [hmap]
  (let [document (Document.)]
    (doseq [[key value] hmap]
      (add-field document (as-str key) value))
    document))

(defn document->map
  [^Document doc only-fields score highlighter ^Explanation explanation]
  (let [m (into {:_score score }
                (for [^IndexableField f (.getFields doc)]
                  (let [str-name (.name f)
                        name (keyword str-name)]
                    (if (or (nil? only-fields)
                            (name only-fields))
                      (let [values (.getValues doc str-name)]
                        (if (= (count values) 1)
                          [name (first values)]
                          [name (vec (rest values))]))))))
        highlighted (highlighter m)]
    (conj
     m
     (when explanation (assoc m :_explain (.toString explanation)))
     (when highlighted (assoc m :_highlight highlighted)))))

(defn get-search-manager ^SearcherManager [index]
  (locking mapping*
    (when (nil? (@mapping* index))
      (swap! mapping* assoc index (SearcherManager. (new-index-directory (root-identifier-path) index)
                                                    nil)))
    (@mapping* index)))

(defn refresh-search-managers []
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

(defn bootstrap-indexes []
  (doseq [f (.listFiles (root-identifier-path))]
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

(defn fragment->map [^TextFragment fragment fidx]
  {:text (.toString fragment)
   :score (.getScore fragment)
   :index fidx
   :frag-num (wall-hack-field (class fragment) :fragNum fragment)
   :text-start-pos (wall-hack-field (class fragment) :textStartPos fragment)
   :text-end-pos (wall-hack-field (class fragment) :textEndPos fragment)})

(defn get-best-fragments [str field highlighter analyzer max-fragments fidx]
  (map #(fragment->map % fidx)
       (.getBestTextFragments ^Highlighter highlighter
                              ^TokenStream (.tokenStream ^Analyzer analyzer
                                                         (as-str field)
                                                         (StringReader. str))
                              ^String str
                              true
                              (int max-fragments))))
(defn make-highlighter
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
                    (if-let [value ((keyword field) m)]
                      (flatten (into [] (for [[fidx str] (indexed (if (vector? value) value [value]))]
                                          (vec (get-best-fragments str
                                                                   field
                                                                   highlighter
                                                                   analyzer
                                                                   max-fragments
                                                                   fidx)))))
                      [])]))))
    (constantly nil)))


(defn get-score-collector ^TopDocsCollector [sort pq-size]
  (TopScoreDocCollector/create pq-size true))

(defn search
  [& {:keys [index query page size explain refresh highlight analyzer facets fields sort]
      :or {page 0, size default-size, explain false,
           refresh false, analyzer nil, facets nil,
           fields nil sort nil}}]
  (if refresh
    (refresh-search-managers))
  (use-searcher index
                (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                  (let [ms-start (time-ms)
                        analyzer (extract-analyzer analyzer)
                        query (parse-query query analyzer)
                        highlighter (make-highlighter query searcher highlight analyzer)
                        pq-size (+ (* page size) size)
                        score-collector (get-score-collector sort pq-size)
                        facet-collector (FacetsCollector.)
                        facet-config (get-facet-config facets)
                        wrap (MultiCollector/wrap
                              ^"[Lorg.apache.lucene.search.Collector;"
                              (into-array Collector
                                          [score-collector
                                           facet-collector]))]
                    (.search searcher
                             query
                             nil
                             wrap)
                    {:total (.getTotalHits score-collector)
                     :facets (if taxo-reader
                               (try
                                 (let [fc (FastTaxonomyFacetCounts. taxo-reader
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
                               {}) ;; no taxo reader, probably problem with open, exception is thrown
                     ;; even though we might fake a facet result
                     ;; it could really surprise the client
                     :hits (into []
                                 (for [^ScoreDoc hit (-> (.topDocs score-collector (* page size)) (.scoreDocs))]
                                   (document->map (.doc searcher (.doc hit))
                                                  fields
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
