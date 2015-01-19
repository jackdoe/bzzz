(ns bzzz.core
  (use ring.adapter.jetty)
  (use bzzz.util)

  (use [clojure.string :only (split join lower-case)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [bzzz.discover :as discover])
  (:require [bzzz.timer :as timer])
  (:require [bzzz.const :as const])
  (:require [bzzz.kv :as kv])
  (:require [bzzz.log :as log])
  (:require [bzzz.index-search :as index-search])
  (:require [bzzz.index-directory :as index-directory])
  (:require [bzzz.index-store :as index-store])
  (:require [bzzz.index-stat :as index-stat])
  (:require [bzzz.analyzer :as analyzer])
  (:require [bzzz.query :as query])
  (:require [bzzz.state :as state])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [org.httpkit.client :as http-client])
  (:gen-class :main true))

(def port* (atom const/default-port))

;; [ "a", ["b","c",["d","e"]]]
(defn search-remote [hosts input c]
  (let [part (if (or (vector? hosts) (list? hosts)) hosts [hosts])
        is-multi (> (count part) 1)
        resolved (discover/peer-resolve (first part))
        args {:timeout (get input :timeout 1000)
              :as :text
              :body (json/write-str (if is-multi
                                      (assoc input :hosts part)
                                      input))}
        callback (fn [{:keys [status headers body error]}]
                   (if error
                     (async/>!! c {:exception (str resolved " " error)})
                     (try
                       (async/>!! c (jr body))
                       (catch Throwable e
                         (async/>!! c {:exception (str resolved " " (ex-str e))})))))]
    (log/trace "<" input "> in part <" part "> to resolved <" resolved ">")
    (try
      (if is-multi
        (http-client/put resolved args callback)
        (http-client/get resolved args callback))
      (catch Throwable e
        (async/>!! c {:exception (str resolved " " (ex-str e))})))))

(defn search-many [hosts input]
  (let [c (async/chan)
        ms-start (time-ms)]
    (doseq [part hosts]
      (search-remote part input c))
    (let [collected (into [] (for [part hosts] (async/<!! c)))]
      (index-search/reduce-collection collected input ms-start))))

(defn stat []
  {:index (index-directory/index-stat)
   :alias @index-directory/alias*
   :identifier @index-directory/identifier*
   :discover-hosts @discover/discover-hosts*
   :peers @discover/peers*
   :timer @timer/time*
   :mem {:heap-size (.totalMemory (Runtime/getRuntime))
         :heap-free (.freeMemory (Runtime/getRuntime))
         :heap-used (- (.totalMemory (Runtime/getRuntime)) (.freeMemory (Runtime/getRuntime)))
         :heap-max-size (.maxMemory (Runtime/getRuntime))}
   :stat (index-stat/get-statistics)
   :next-gc @discover/next-gc*})

(defn assoc-index [input uri]
  (let [path (subs uri 1)]
    (if (= 0 (count path))
      input
      (assoc input :index path))))

(defn work [method uri input]
  (let [input (assoc-index input uri)]
    (log/debug "received request" method input)
    (condp = method
      :post (index-store/store input)
      :delete (index-store/delete-from-query (:index input)
                                             (:query input))
      :get (case uri
             "/_stat" (stat)
             "/_gc" (System/gc)
             "/_log_inc" (swap! log/level* inc)
             "/_log_dec" (swap! log/level* dec)
             "/_stack" (into {} (for [[^Thread t traces] (Thread/getAllStackTraces)]
                                  [(str (.toString t) "-" (.toString ^Thread$State (.getState t)))
                                   (into [] (for [^StackTraceElement s traces]
                                              (.toString s)))]))
             "/favicon.ico" "" ;; XXX

             "/_kv/store"  (kv/store input)
             "/_kv/search" (kv/search input)

             "/_state/temp_assoc_in"           (state/temp-assoc-in (:data input))
             "/_state/temp_empty"              (state/temp-empty)
             "/_state/ro_replace_with_temp"    (state/ro-replace-with-temp)
             "/_state/ro_merge_with_temp"      (state/ro-merge-with-temp)
             "/_state/ro_deep_merge_with_temp" (state/ro-deep-merge-with-temp)
             "/_state/ro_merge"      (state/ro-merge input)
             "/_state/ro_deep_merge" (state/ro-deep-merge input)
             "/_state/ro_rename_key" (state/ro-rename-key (:from_key input) (:to_key input))

             (index-search/search input))
      :put (search-many (:hosts input) (dissoc input :hosts))
      :patch (discover/merge-discover-hosts (get input :discover-hosts {}))
      (throw (Throwable. "unexpected method" method)))))

(defn handler [request]
  (let [t0 (time-ms)
        stat-key (str "http-" (lower-case (as-str (:request-method request))))]
    (try
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/write-str (work (:request-method request)
                                   (:uri request)
                                   (json/read-str (slurp-or-default (:body request) "{}") :key-fn keyword)))}
      (catch Throwable e
        (let [ex (ex-str e)]
          (index-stat/update-error index-stat/total stat-key)
          (log/warn request "->" ex)
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (json/write-str {:exception ex})}))
      (finally (index-stat/update-took-count index-stat/total
                                             stat-key
                                             (time-took t0))))))

(defn port-validator [port]
  (< 0 port 0x10000))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :id :port
    :default const/default-port
    :parse-fn #(Integer/parseInt %)
    :validate [ #(port-validator %) "Must be a number between 0 and 65536"]]
   ["-x" "--acceptable-discover-time-diff NUM-IN-SECONDS" "only consider discovered hosts who refreshed witihin the last X seconds"
    :id :acceptable-discover-time-diff
    :default const/default-acceptable-discover-time-diff
    :parse-fn #(Integer/parseInt %)
    :validate [ #(> % 0) "Must be a number > 0"]]
   ["-u" "--allow-unsafe-queries" "allow unsafe queries"
    :id :allow-unsafe-queries
    :default const/default-allow-unsafe-queries]
   ["-g" "--gc-interval NUM-IN-SECONDS" "do GC approx every N seconds (it will be N/2 + rand(N/2))"
    :id :gc-interval
    :default 0
    :parse-fn #(Integer/parseInt %)
    :validate [ #(>= % 0) "Must be a number >= 0"]]
   ["-v" "--verbose LEVEL" "set log level"
    :id :verbose
    :default 1
    :parse-fn #(Integer/parseInt %)
    :validate [ #(>= % 0) "Must be a number >= 0"]]
   ["-i" "--identifier 'string'" "identifier used for auto-discover and resolving"
    :id :identifier
    :default const/default-identifier]
   ["-b" "--bind 'string'" "bind only on specific ip address"
    :id :bind
    :default "0.0.0.0"]
   ["-f" "--peers-file fetch file" "fetch @discover/peers* from file every second"
    :id :peers-file
    :default nil]
   ["-o" "--hosts host:port,host:port" "initial hosts that will be queried for identifiers for auto-resolve"
    :id :discover-hosts
    :default ""]
   ["-d" "--directory DIRECTORY" "directory that will contain all the indexes"
    :id :directory
    :default const/default-root]])

(defn -main [& args]
  (let [{:keys [options errors]} (parse-opts args cli-options)]
    (when (not (nil? errors))
      (log/fatal errors)
      (System/exit 1))

    (when (and (:peers-file options) (or (> (:gc-interval options) 0) (> (count (:discover-hosts options)) 0)))
      (log/fatal "cannot have --peers-file AND (--gc-interval OR --discover-hosts)")
      (System/exit 1))

    ;; split uses java.util.regex.Pattern/split so
    ;; bzzz.core=> (split "" #",")
    ;;[""]
    (discover/merge-discover-hosts (into {} (for [host (filter #(> (count %) 0)
                                                               (split (:discover-hosts options) #","))]
                                              [host true])))
    (reset! discover/acceptable-discover-time-diff* (:acceptable-discover-time-diff options))

    (reset! discover/gc-interval* (:gc-interval options))
    (reset! index-directory/identifier* (keyword (:identifier options)))
    (reset! query/allow-unsafe-queries* (:allow-unsafe-queries options))
    (reset! index-directory/root* (:directory options))
    (reset! log/level* (:verbose options))
    (reset! port* (:port options))
    (index-directory/initial-read-alias-file)
    (index-stat/initial-setup)

    (.addShutdownHook (Runtime/getRuntime) (Thread. #(index-directory/shutdown)))

    (log/info options)

    (every 5000 #(index-directory/refresh-search-managers) (mk-pool) :desc "search refresher")
    (every 1000 #(timer/tick) (mk-pool) :desc "timer")
    (every 1000 #(discover/periodic @port* (:peers-file options)) (mk-pool) :desc "periodic discover")

    (repeatedly
     (try
       (run-jetty handler {:port @port*, :host (:bind options)})
       (catch Throwable e
         (do
           (log/warn (ex-str e))
           (Thread/sleep 10000)))))))
