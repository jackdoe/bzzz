(ns bzzz.core
  (use ring.adapter.jetty)
  (use overtone.at-at)
  (use clojure.stacktrace)
  (:require [bzzz.spam :as spam])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
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
                                     Scorer TermQuery SearcherManager)
           (org.apache.lucene.util Version AttributeSource)
           (org.apache.lucene.store NIOFSDirectory RAMDirectory Directory))
  (:gen-class))


(def ^{:dynamic true} *version* Version/LUCENE_CURRENT)
(def ^{:dynamic true} *analyzer* (WhitespaceAnalyzer. *version*))
(def default-root "/tmp/BZZZ")
(def default-port 3000)
(def default-spam-port [3001])

(def root* (atom default-root))
(def port* (atom default-port))
(def spam-port* (atom default-spam-port))
(def mapping* (atom {}))
(def z-state* (atom {}))
(def cron-tp (mk-pool))

(defn substring? [sub st]
 (not= (.indexOf st sub) -1))

(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
    (str x)))

(defn current-stamp []
  (int (/ (System/currentTimeMillis) 1000)))

(defn udp-receive-message-and-update-z-state [m]
  (locking z-state*
    (try
      (let [decoded (json/read-str (:message m))]
        (doseq [[name count] decoded]
          (swap! z-state*
                 (assoc-in [(as-str name) (:peer m)]
                           {:count count
                            :stamp (current-stamp)}))))

      (catch Exception e
        (print-cause-trace e)))))




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
                     (= str-key "id"))
                  Field$Store/YES
                  Field$Store/NO)
                (if (or
                     (substring? "_index" str-key)
                     (= str-key "id"))
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
      (if (:id m)
        (.updateDocument writer (Term. "id" (as-str (:id m))) (map->document m))
        (.addDocument writer (map->document m))))
    (.commit writer)
    (.forceMerge writer 1)
    (.close writer)
    true))

(defn get-search-manager ^SearcherManager [name]
  (locking mapping*
    (when (nil? (@mapping* name))
      (swap! mapping* assoc name (SearcherManager. (new-index-directory name)
                                                   nil)))
    (@mapping* name)))

(defn refresh-search-managers []
  (locking mapping*
    (doseq [[name manager] @mapping*]
      (println "refreshing: " name " " manager)
      (.maybeRefresh manager))))

(defn bootstrap-indexes []
  (doseq [f (.listFiles (File. (as-str @root*)))]
    (if (.isDirectory f)
      (get-search-manager (.getName f)))))

(defn use-searcher-from-search-manager [name f]
  (let [manager (get-search-manager name)
        searcher (.acquire manager)]
    (try
      (f searcher)
      (finally (.release manager searcher)))))

(defn search [name query size]
  (use-searcher-from-search-manager name (fn [searcher]
                             (let [parser (QueryParser. *version* "_default_" *analyzer*)
                                   query (.parse parser query)
                                   hits (.search searcher query (int size))
                                   m (into [] (for [hit (.scoreDocs hits)]
                                                (document->map (.doc ^IndexSearcher searcher (.doc ^ScoreDoc hit))
                                                               (.score ^ScoreDoc hit))))]
                               { :total (.totalHits hits), :hits m }))))
(defn work [method input]
  (println method input)
  (if (= :post method)
    (store (:index input) (:documents input))
    (search (:index input) (:query input) (:size input))))

(defn handler [request]
  (try
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/write-str (work (:request-method request) (json/read-str (slurp (:body request)) :key-fn keyword)))}
    (catch Exception e
      (print-cause-trace e)
      {:status 500
       :headers {"Content-Type" "text/plain"}
       :body (str "exception:" (.getMessage e))})))
(defn port-validator [port]
  (< 0 port 0x10000))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :id :port
    :default default-port
    :parse-fn #(Integer/parseInt %)
    :validate [ #(port-validator %)"Must be a number between 0 and 65536"]]
   ["-s" "--spam-port SPAM-PORT" "the port used for udp spam state, will listen only on the first one"
    :required true
    :id :spam-port
    :validate [(fn [param]
                 (and
                  (> (count param) 0)
                  (= 0 (count (filter (fn [x] (not (port-validator x))) param))))) "must be a comma separated list with numbers between 0 and 65536 (like 1234,1235...etc)"]
    :parse-fn (fn [param] (into [] (map #(Integer/parseInt %) (clojure.string/split param #","))))]
   ["-d" "--directory DIRECTORY" "directory that will contain all the indexes"
    :id :directory
    :default default-root]])

(defn main [& args]
  (let [{:keys [options errors]} (parse-opts args cli-options)]
    (when (not (nil? errors))
      (println errors)
      (System/exit 1))
    (println options)
    (reset! spam-port* (:spam-port options))
    (reset! root* (:directory options))
    (reset! port* (:port options)))
  (bootstrap-indexes)
  (every 5000 #(refresh-search-managers) cron-tp :desc "search refresher")
  (println "starting bzzzz on port" @port* "with index root directory" @root* "and spam ports:" @spam-port*)
  (async/go #(spam/start "255.255.255.255" (first @spam-port*)))
  (run-jetty handler {:port @port*}))
