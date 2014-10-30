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

(defn use-writer-all [index callback]
  (doseq [name (index-name-matching index)]
    (use-writer name callback)))

(defn bootstrap-indexes []
  (doseq [name (sub-directories)]
    (use-writer name (fn [writer taxo]
                       ;; do nothing, just open/close the taxo directory
                       ))
    (get-search-manager name)))

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
  [& {:keys [index documents analyzer facets shard]
      :or {analyzer nil facets {} shard 0}}]
  (if analyzer
    (reset! analyzer* (parse-analyzer analyzer)))
  (use-writer (sharded index shard) (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]
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
  (let [query (parse-query input (extract-analyzer nil))]
    (use-writer-all index (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]
                            (.deleteDocuments writer
                                              ^"[Lorg.apache.lucene.search.Query;"
                                              (into-array Query [query]))))
    {index (.toString query)}))

(defn delete-all [index]
  (use-writer-all index (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo] (.deleteAll writer))))

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

(defn limit [input hits sort-key]
  (let [size (default-to (:size input) default-size)
        sorted (sort-by sort-key #(compare %2 %1) hits)]
    (if (and  (> (count hits) size)
              (default-to (:enforce-limits input) true))
      (subvec (vec sorted) 0 size)
      sorted)))

(defn concat-facets [big small]
  (if (not big)
    small ;; initial reduce
    (into big
          (for [[k v] small]
            (if-let [big-list (get big k)]
              [k (concat v big-list)]
              [k v])))))

(defn input-facet-settings [input dim]
  (let [global-ef (default-to (:enforce-limits input) true)]
    (if-let [config (get-in [:facets (keyword dim)] input)]
      (if (contains? config :enforce-limits)
        config
        (assoc config :enforce-limits global-ef))
      {:enforce-limits global-ef})))

(defn merge-facets [facets]
  ;; produces not-sorted output
  (into {}
        (for [[k v] (default-to facets {})]
          [(keyword k) (vals (reduce (fn [sum next]
                                       (let [l (:label next)]
                                         (if (contains? sum l)
                                           (update-in sum [l :count] + (:count next))
                                           (assoc sum l next))))
                                     {}
                                     v))])))

(defn merge-and-limit-facets [input facets]
  (into {} (for [[k v] (merge-facets facets)]
             ;; this is broken by design
             ;; :__shard_2 {:facets {:name [{:label "jack doe"
             ;;                              :count 100}
             ;;                             {:label "john doe"
             ;;                              :count 10}]}}
             ;;                          ;; -----<cut>-------
             ;;                          ;; {:label "foo bar"
             ;;                          ;; :count 8}
             ;;
             ;; :__shard_3 {:facets {:name [{:label "foo bar"
             ;;                              :count 9}]}}}
             ;;
             ;; so when the multi-search merge happens
             ;; with size=2,it will actully return only
             ;; 'jack doe(100)' and 'john doe(10)' even though
             ;; the actual count of 'foo bar' is 17, because
             ;; __shard_2 actually didnt even send 'foo bar'
             ;; because of the size=2 cut
             [k (limit (input-facet-settings input (keyword k))
                       v
                       :count)])))


(defn result-reducer [sum next]
  (let [next (if (future? next)
               (try
                 @next
                 (catch Throwable e
                   {:exception (as-str e)}))
               next)
        ex (if (:exception next)
             (if-not (:can-return-partial sum)
               (throw (Throwable. (as-str (:exception next))))
               (do
                 (log/info (str "will send partial response: " (as-str (:exception next))))
                 (as-str (:exception next))))
             nil)]
    (-> sum
        (update-in [:failed] conj-if ex)
        (update-in [:failed] concat-if (:failed next))
        (update-in [:facets] concat-facets (default-to (:facets next) {}))
        (update-in [:total] + (default-to (:total next) 0))
        (update-in [:hits] concat (default-to (:hits next) [])))))

(defn reduce-collection [collection input ms-start]
  (let [result (reduce result-reducer
                       {:total 0
                        :hits []
                        :facets {}
                        :took -1
                        :failed []
                        :can-return-partial (default-to (:can-return-partial input) false)}
                       collection)]
    (-> result
        (assoc-in [:facets] (merge-and-limit-facets input (:facets result)))
        (assoc-in [:hits] (limit input (:hits result) :_score))
        (assoc-in [:took] (time-took ms-start)))))

(defn shard-search
  [& {:keys [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader
             ^Query query ^Analyzer analyzer
             page size explain highlight facets fields facet-config]
      :or {page 0, size default-size, explain false,
           analyzer nil, facets nil, fields nil}}]
  (let [ms-start (time-ms)
        highlighter (make-highlighter query searcher highlight analyzer)
        pq-size (+ (* page size) size)
        score-collector (get-score-collector sort pq-size)
        facet-collector (FacetsCollector.)

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
     :took (time-took ms-start)}))

(defn search [input]
  (let [ms-start (time-ms)
        index (need :index input "need index")
        page (default-to (:page input) 0)
        size (default-to (:size input) default-size)
        explain (default-to (:explain input) false)
        highlight (:highlight input)
        fields (:fields input)
        analyzer (extract-analyzer (:analyzer input))
        query (parse-query (:query input) analyzer)
        facets (:facets input)
        facet-config (get-facet-config facets)
        futures (into [] (for [shard (index-name-matching index)]
                           (future (use-searcher shard
                                                 (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                                   (shard-search :searcher searcher
                                                                 :taxo-reader taxo-reader
                                                                 :analyzer analyzer
                                                                 :facet-config facet-config
                                                                 :highlight highlight
                                                                 :query query
                                                                 :page page
                                                                 :size size
                                                                 :facets facets
                                                                 :explain explain
                                                                 :fields fields))))))]
    (reduce-collection futures input ms-start)))

(defn shutdown []
  (locking mapping*
    (log/info "executing shutdown hook, current mapping: " @mapping*)
    (reset-search-managers)
    (log/info "mapping after cleanup: " @mapping*)))

(defn index-stat []
  (into {} (for [[name searcher] @mapping*]
             (let [sample (search {:index name
                                   :query {:random-score {:query {:match-all {}}}}})]
               [name (use-searcher name
                                   (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                     (let [reader (.getIndexReader searcher)]
                                       {:docs (.numDocs reader)
                                        :sample sample
                                        :has-deletions (.hasDeletions reader)})))]))))
