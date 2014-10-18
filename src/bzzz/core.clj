(ns bzzz.core
  (use ring.adapter.jetty)
  (use overtone.at-at)
  (use clojure.stacktrace)
  (use [clojure.repl :only (pst)])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as http-client])
  (:require [clojure.tools.logging :as log])
  (:import (java.io StringReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.core WhitespaceAnalyzer)
           (org.apache.lucene.document Document Field Field$Index Field$Store)
           (org.apache.lucene.index IndexWriter IndexReader Term
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery IndexSearcher Query ScoreDoc
                                     Scorer TermQuery SearcherManager
                                     Explanation ComplexExplanation
                                     Collector TopScoreDocCollector TopDocsCollector)
           (org.apache.lucene.util Version AttributeSource)
           (org.apache.lucene.store NIOFSDirectory RAMDirectory Directory))
  (:gen-class :main true))

(set! *warn-on-reflection* true)
(def ^{:dynamic true} *version* Version/LUCENE_CURRENT)
(def ^{:dynamic true} *analyzer* (WhitespaceAnalyzer. *version*))
(def id-field "id")
(def default-root "/tmp/BZZZ")
(def default-port 3000)
(def root* (atom default-root))
(def port* (atom default-port))
(def mapping* (atom {}))
(def cron-tp (mk-pool))

(defn mapply [f & args] (apply f (apply concat (butlast args) (last args))))
(defn substring? [^String sub ^String st]
  (not= (.indexOf st sub) -1))

(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
    (str x)))

(defmacro with-err-str
  "Evaluates exprs in a context in which *err* is bound to a fresh
  StringWriter.  Returns the string created by any nested printing
  calls."
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
                (IndexWriterConfig. *version* *analyzer*)))

(defn new-index-reader ^IndexReader [name]
  (with-open [writer (new-index-writer name)]
    (DirectoryReader/open ^IndexWriter writer false)))

(defn- add-field [document key value]
  (let [ str-key (as-str key) ]
    (.add ^Document document
          (Field. str-key (as-str value)
                  (if (or
                       (substring? "_store" str-key)
                       (= str-key id-field))
                    Field$Store/YES
                    Field$Store/NO)
                  (if (or
                       (substring? "_index" str-key)
                       (= str-key id-field))
                    Field$Index/ANALYZED
                    Field$Index/NOT_ANALYZED)))))

(defn map->document
  [hmap]
  (let [document (Document.)]
    (doseq [[key value] hmap]
      (add-field document key value))
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

(defn store
  [index maps]
  (use-writer index (fn [^IndexWriter writer]
                      (doseq [m maps]
                        (if (:id m)
                          (.updateDocument writer ^Term (Term. ^String id-field (as-str (:id m))) (map->document m))
                          (.addDocument writer (map->document m))))
                      { index true })))

(defn delete-from-query
  [index query]
  (use-writer index (fn [^IndexWriter writer]
                      (let [parser (QueryParser. *version* id-field *analyzer*)]
                        ;; method is deleteDocuments(Query...)
                        (.deleteDocuments writer
                                          (into-array Query [^Query (.parse parser query)])))
                      { index true })))

(defn delete-all [index]
  (use-writer index (fn [^IndexWriter writer] (.deleteAll writer))))

(defn search
  [& {:keys [index query default-field default-operator page size explain]
      :or {default-field "_default_", default-operator "and", page 0, size 20, explain false}}]
  (use-searcher index
                (fn [^IndexSearcher searcher]
                  (let [parser (doto
                                   (QueryParser. *version* (as-str default-field) *analyzer*)
                                 (.setDefaultOperator (case default-operator
                                                        "and" QueryParser/AND_OPERATOR
                                                        "or"  QueryParser/OR_OPERATOR)))
                        query (.parse parser query)
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
    :post (store (:index input) (:documents input))
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
      (print-cause-trace e)
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
