(ns bzzz.core
  (use ring.adapter.jetty)
  (use [clojure.repl :only (pst)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as http-client])
  (:require [clojure.tools.logging :as log])
  (:import (java.io StringReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.analysis.miscellaneous PerFieldAnalyzerWrapper)
           (org.apache.lucene.analysis.core WhitespaceAnalyzer KeywordAnalyzer)
           (org.apache.lucene.document Document Field Field$Index Field$Store)
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery IndexSearcher Query ScoreDoc
                                     Scorer TermQuery SearcherManager
                                     Explanation ComplexExplanation
                                     MatchAllDocsQuery
                                     Collector TopScoreDocCollector TopDocsCollector)
           (org.apache.lucene.util Version AttributeSource)
           (org.apache.lucene.store NIOFSDirectory RAMDirectory Directory))
  (:gen-class :main true))

(declare parse-query)
(set! *warn-on-reflection* true)
(def ^{:dynamic true} *version* Version/LUCENE_CURRENT)
(def id-field "id")
(def default-root "/tmp/BZZZ")
(def default-port 3000)
(def root* (atom default-root))
(def port* (atom default-port))
(def mapping* (atom {}))
(def cron-tp (mk-pool))
(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
    (str x)))

(defn obj-to-lucene-analyzer [obj]
  (let [name (:use obj)]
    (case (as-str name)
      "whitespace" (WhitespaceAnalyzer. *version*)
      "keyword" (KeywordAnalyzer.)
      "standard" (StandardAnalyzer. *version*))))

(defn parse-analyzer [input]
  (PerFieldAnalyzerWrapper. (WhitespaceAnalyzer. *version*)
                            (into { id-field (KeywordAnalyzer.) }
                                  (for [[key value] input]
                                    { (as-str key) (obj-to-lucene-analyzer value) }))))

(def analyzer* (atom (parse-analyzer {}))) ;; FIXME: move to top

(defn mapply [f & args] (apply f (apply concat (butlast args) (last args))))
(defn substring? [^String sub ^String st]
  (not= (.indexOf st sub) -1))

(defmacro with-err-str
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*err* s#]
       ~@body
       (str s#))))

(defn acceptable-index-name [name]
  (clojure.string/replace name #"[^a-zA-Z_0-9-]" ""))

(defn new-index-directory ^Directory [name]
  (NIOFSDirectory. (File. (File. (as-str @root*)) (as-str (acceptable-index-name name)))))

(defn new-index-writer ^IndexWriter [name]
  (IndexWriter. (new-index-directory name)
                (IndexWriterConfig. *version* @analyzer*)))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))

(defn index? [name]
  (if (or (substring? "_index" name)
          (= name id-field))
    true
    false))

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

(defn- add-field [document key value]
  (let [ str-key (as-str key) ]
    (.add ^Document document
          (Field. str-key (as-str value)
                  (if (or
                       (substring? "_store" str-key)
                       (= str-key id-field))
                    Field$Store/YES
                    Field$Store/NO)
                  (if (index? str-key)
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
  [^Document doc score ^Explanation explanation]
  (conj
   (into {:_score score }
         (for [^Field f (.getFields doc)]
           [(keyword (.name f)) (.stringValue f)]))
   (when explanation { :_explain (.toString explanation) })))

(defn get-search-manager ^SearcherManager [index]
  (locking mapping*
    (when (nil? (@mapping* index))
      (swap! mapping* assoc index (SearcherManager. (new-index-directory index)
                                                    nil)))
    (@mapping* index)))

(defn refresh-search-managers []
  (locking mapping*
    (doseq [[index ^SearcherManager manager] @mapping*]
      (log/info "refreshing: " index " " manager)
      (.maybeRefresh manager))))

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

(defn parse-lucene-query-parser
  ^Query
  [& {:keys [query default-field default-operator analyzer]
      :or {default-field "_default_", default-operator "and" analyzer nil}}]
  (let [parser (doto
                   (QueryParser. *version* (as-str default-field) (if (nil? analyzer)
                                                                    @analyzer*
                                                                    (parse-analyzer analyzer)))
                 (.setDefaultOperator (case (as-str default-operator)
                                        "and" QueryParser/AND_OPERATOR
                                        "or"  QueryParser/OR_OPERATOR)))]
    (.parse parser query)))

(defn parse-bool-query
  ^Query
  [& {:keys [must should minimum-should-match boost]
      :or {minimum-should-match 0 should [] must [] boost 1}}]
  (let [top ^BooleanQuery (BooleanQuery. true)]
    (doseq [q must]
      (.add top (parse-query q) BooleanClause$Occur/MUST))
    (doseq [q should]
      (.add top (parse-query q) BooleanClause$Occur/SHOULD))
    (.setMinimumNumberShouldMatch top minimum-should-match)
    (.setBoost top boost)
    top))

(defn parse-term-query
  ^Query
  [& {:keys [field value boost]
      :or {boost 1}}]
  (let [q (TermQuery. (Term. ^String field ^String value))]
    (.setBoost q boost)
    q))

(defn parse-query-fixed ^Query [key val]
  (case (as-str key)
    "query-parser" (mapply parse-lucene-query-parser val)
    "term" (mapply parse-term-query val)
    "match-all" (MatchAllDocsQuery.)
    "bool" (mapply parse-bool-query val)))

;; query => { bool => {}}
(defn parse-query ^Query [input]
  (if (string? input)
    (parse-lucene-query-parser :query input)
    (if (= (count input) 1)
      (let [[key val] (first input)]
        (parse-query-fixed key val))
      (let [top (BooleanQuery. false)]
        (doseq [[key val] input]
          (.add top (parse-query-fixed key val) BooleanClause$Occur/MUST))
        top))))

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
                      (let [query (parse-query input)]
                        (.deleteDocuments writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query [query]))
                        { index (.toString query) }))))

(defn delete-all [index]
  (use-writer index (fn [^IndexWriter writer] (.deleteAll writer))))

(defn search
  [& {:keys [index query page size explain]
      :or {page 0, size 20, explain false}}]
  (use-searcher index
                (fn [^IndexSearcher searcher]
                  (let [query (parse-query query)
                        pq-size (+ (* page size) size)
                        collector ^TopDocsCollector (TopScoreDocCollector/create pq-size true)]
                    (.search searcher query collector)
                    {:total (.getTotalHits collector)
                     :hits (into []
                                 (for [^ScoreDoc hit (-> (.topDocs collector (* page size)) (.scoreDocs))]
                                   (document->map (.doc searcher (.doc hit))
                                                  (.score hit)
                                                  (when explain
                                                    (.explain searcher query (.doc hit))))))}))))

;; [ "a", ["b","c",["d","e"]]]
(defn search-remote [hosts input]
  (let [part (if (or (vector? hosts) (list? hosts)) hosts [hosts])
        args {:accept :json
              :as :json
              :body-encoding "UTF-8"
              :body (json/write-str input)
              :socket-timeout 1000
              :conn-timeout 1000}]
    (log/info "<" input "> in part <" part ">")
    (if (> (count part) 1)
      (:body (http-client/put (first part) args))
      (:body (http-client/get (first part) args)))))

(defn search-many [hosts input]
  (let [c (async/chan)]
    (doseq [part hosts]
      (async/go (async/>! c (search-remote part input))))
    (let [ collected (into [] (for [part hosts] (async/<!! c)))]
      (reduce (fn [sum next]
                (-> sum
                    (update-in [:total] + (:total next))
                    (update-in [:hits] concat (:hits next))))
              { :total 0, :hits [] }
              collected))))

(defn work [method input]
  (log/info "received request" method input)
  (condp = method
    :post (store (:index input) (:documents input) (:analyzer input))
    :delete (delete-from-query (:index input) (:query input))
    :get (mapply search input)
    :put (search-many (:hosts input) (dissoc input :hosts))
    (throw (Throwable. "unexpected method" method))))

(defn handler [request]
  (try
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/write-str (work (:request-method request) (json/read-str (slurp (:body request)) :key-fn keyword)))}
    (catch Exception e
      (println (with-err-str (pst e 36)))
      {:status 500
       :headers {"Content-Type" "text/plain"}
       :body (with-err-str (pst e 36))})))

(defn port-validator [port] (< 0 port 0x10000))
(def cli-options
  [["-p" "--port PORT" "Port number"
    :id :port
    :default default-port
    :parse-fn #(Integer/parseInt %)
    :validate [ #(port-validator %) "Must be a number between 0 and 65536"]]
   ["-d" "--directory DIRECTORY" "directory that will contain all the indexes"
    :id :directory
    :default default-root]])

(defn shutdown []
  (locking mapping*
    (log/info "executing shutdown hook, current mapping: " @mapping*)
    (doseq [[index ^SearcherManager manager] @mapping*]
      (log/info "\tclosing: " index " " manager)
      (.close manager))
    (reset! mapping* {})
    (log/info "mapping after cleanup: " @mapping*)))

(defn -main [& args]
  (let [{:keys [options errors]} (parse-opts args cli-options)]
    (when (not (nil? errors))
      (log/fatal errors)
      (System/exit 1))
    (log/info options)
    (reset! root* (:directory options))
    (reset! port* (:port options)))
  (bootstrap-indexes)
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(shutdown)))
  (every 5000 #(refresh-search-managers) cron-tp :desc "search refresher")
  (log/info "starting bzzzz on port" @port* "with index root directory" @root*)
  (run-jetty handler {:port @port*}))
