(ns bzzz.core
  (use ring.adapter.jetty)
  (use bzzz.util)
  (use bzzz.const)
  (use [clojure.string :only (split join lower-case)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [bzzz.const :as const])
  (:require [bzzz.index-search :as index-search])
  (:require [bzzz.index-directory :as index-directory])
  (:require [bzzz.index-store :as index-store])
  (:require [bzzz.index-stat :as index-stat])
  (:require [bzzz.analyzer :as analyzer])
  (:require [bzzz.query :as query])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [org.httpkit.client :as http-client])
  (:require [clojure.tools.logging :as log])
  (:import (java.net URL))
  (:gen-class :main true))

(def timer* (atom 0))
(def port* (atom const/default-port))
(def acceptable-discover-time-diff* (atom const/default-acceptable-discover-time-diff))
(def discover-interval* (atom const/default-discover-interval))
(def gc-interval* (atom const/default-gc-interval))
(def next-gc* (atom 0))
(def discover-hosts* (atom {}))
(def peers* (atom {}))

(defn rescent? [than]
  (< (- @timer* (get (second than) :update 0)) @acceptable-discover-time-diff*))

(defn not-doing-gc? [than]
  (let [diff (- (get (second than) :next-gc-at (+ @timer* 100000)) @timer*)]
    (not (< (abs diff) 2)))) ;; regardless if we are 2 seconds before gc
                             ;; or 2 seconds after, try to skip this host

(defn possible-hosts [list]
  (let [rescent (filter rescent? list)]
    (let [not-doing-gc (filter not-doing-gc? rescent)]
      (if (= 0 (count not-doing-gc))
        (do
          (log/debug "found host after ignoring the gcing ones, dump:" list @timer* @discover-hosts* @peers*)
          (first (rand-nth rescent)))
        (first (rand-nth not-doing-gc))))))

(defn peer-resolve [identifier]
  (let [t0 (time-ms)
        resolved (if-let [all-possible ((keyword identifier) @peers*)]
                   (if-let [host (possible-hosts all-possible)]
                     host
                     (throw (Throwable. (str "cannot find possible hosts for:" (as-str identifier)))))
                   ;; nothing matches the identifier
                   identifier)]
    (index-stat/update-took-count index-stat/total "peer-resolve" (time-took t0))
    resolved))

;; [ "a", ["b","c",["d","e"]]]
(defn search-remote [hosts input c]
  (let [part (if (or (vector? hosts) (list? hosts)) hosts [hosts])
        is-multi (> (count part) 1)
        args {:timeout (get input :timeout 1000)
              :as :text
              :body (json/write-str (if is-multi
                                      (assoc input :hosts part)
                                      input))}
        callback (fn [{:keys [status headers body error]}]
                   (if error
                     (async/>!! c {:exception error})
                     (try
                       (async/>!! c (jr body))
                       (catch Throwable e
                         (async/>!! c {:exception (ex-str e)})))))
        resolved (peer-resolve (first part))]
    (log/trace "<" input "> in part <" part "> to resolved <" resolved ">")
    (try
      (if is-multi
        (http-client/put resolved args callback)
        (http-client/get resolved args callback))
      (catch Throwable e
        (async/>!! c {:exception (ex-str e)})))))

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
   :analyzer (analyzer/analyzer-stat)
   :identifier @index-directory/identifier*
   :discover-hosts @discover-hosts*
   :peers @peers*
   :timer @timer*
   :stat (index-stat/get-statistics)
   :next-gc @next-gc*})

(defn validate-discover-url [x]
  (let [url (URL. ^String (as-str x))]
    (join "://" [(.getProtocol url) (join ":" [(.getHost url) (.getPort url)])])))

(defn merge-discover-hosts [x]
  (try
    (if x
      (locking discover-hosts*
        (swap! discover-hosts* merge (into {} (for [host (keys x)]
                                                [(validate-discover-url host) true])))))
    (catch Exception e
      (log/warn "merge-discovery-hosts" x (.getMessage e))))
  {:identifier @index-directory/identifier*
   :discover-hosts @discover-hosts*
   :next-gc @next-gc*})

(defn assoc-index [input uri]
  (let [path (subs uri 1)]
    (if (= 0 (count path))
      input
      (assoc input :index path))))

(defn work [method uri input]
  (let [input (assoc-index input uri)]
    (log/debug "received request" method input)
    (condp = method
      :post (mapply index-store/store input)
      :delete (index-store/delete-from-query (:index input)
                                             (:query input))
      :get (case uri
             "/_stat" (stat)
             "/favicon.ico" "" ;; XXX
             (index-search/search input))
      :put (search-many (:hosts input) (dissoc input :hosts))
      :patch (merge-discover-hosts (get input :discover-hosts {}))
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

(defn update-discovery-state [host c str-state]
  (try
    (log/trace "sending discovery query to" host)
    (http-client/patch host
                       {:body str-state
                        :as :text
                        :keepalive -1
                        :timeout 500}
                       (fn [{:keys [status headers body error]}]
                         (if error
                           (log/trace error)
                           (let [info (jr body)
                                 host-identifier (keyword (need :identifier info "need identifier"))
                                 host-next-gc (+ @timer* (get info :next-gc 1000000))
                                 remote-discover-hosts (:discover-hosts info)]
                             (log/trace "updating" host "with identifier" host-identifier)
                             (locking peers*
                               (swap! peers*
                                      assoc-in [host-identifier host] {:update @timer*
                                                                       :next-gc-at host-next-gc}))
                             (merge-discover-hosts remote-discover-hosts)))
                         (async/>!! c true)))
    (catch Exception e
      (index-stat/update-error index-stat/total "update-discovery")
      (log/trace "update-discovery-state" host (.getMessage e))
      (async/>!! c false))))

(defn discover []
  ;; just add localhost:port to the possible servers for specific identifier
  ;; hackish, just POC
  (locking peers*
    (swap! peers*
           assoc-in [@index-directory/identifier*
                     (join ":" ["http://localhost" (as-str @port*)])] {:update @timer*
                                                                       :next-gc-at (+ @timer* @next-gc*)}))
  (let [t0 (time-ms)
        c (async/chan)
        hosts @discover-hosts*
        str-state (json/write-str {:discover-hosts @discover-hosts*})]
    (doseq [[host unused] hosts]
      (update-discovery-state host c str-state))
    (doseq [host hosts]
      (async/<!! c))
    (index-stat/update-took-count index-stat/total "discover" (time-took t0))))

(defn attempt-gc []
  (if (<= @next-gc* 0)
    (do
      (let [t0 (time-ms)]
        (System/gc)
        (index-stat/update-took-count index-stat/total "gc" (time-took t0)))
      (let [half (/ @gc-interval* 2)]
        (reset! next-gc* (+ half (int (rand half))))))
    (swap! next-gc* dec)))

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
   ["-r" "--discover-interval NUM-IN-SECONDS" "exchange information with the discover hosts every N seconds"
    :id :discover-interval
    :default const/default-discover-interval
    :parse-fn #(Integer/parseInt %)
    :validate [ #(>= % 0) "Must be a number >= 0"]]
   ["-g" "--gc-interval NUM-IN-SECONDS" "do GC approx every N seconds (it will be N/2 + rand(N/2))"
    :id :gc-interval
    :default const/default-gc-interval
    :parse-fn #(Integer/parseInt %)
    :validate [ #(>= % 0) "Must be a number >= 0"]]
   ["-i" "--identifier 'string'" "identifier used for auto-discover and resolving"
    :id :identifier
    :default const/default-identifier]
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
    ;; split uses java.util.regex.Pattern/split so
    ;; bzzz.core=> (split "" #",")
    ;;[""]
    (merge-discover-hosts (into {} (for [host (filter #(> (count %) 0)
                                                      (split (:discover-hosts options) #","))]
                                     [host true])))
    (reset! acceptable-discover-time-diff* (:acceptable-discover-time-diff options))
    (reset! discover-interval* (:discover-interval options))
    (reset! gc-interval* (:gc-interval options))
    (reset! index-directory/identifier* (keyword (:identifier options)))
    (reset! query/allow-unsafe-queries* (:allow-unsafe-queries options))
    (reset! index-directory/root* (:directory options))
    (reset! port* (:port options)))
  (index-directory/initial-read-alias-file)
  (index-stat/initial-setup)
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(index-directory/shutdown)))
  (log/info "starting bzzz --identifier" (as-str @index-directory/identifier*) "--port" @port* "--directory" @index-directory/root* "--hosts" @discover-hosts* "--acceptable-discover-time-diff" @acceptable-discover-time-diff* "--discover-interval" @discover-interval* "--gc-interval" @gc-interval* "--allow-unsafe-queries" @query/allow-unsafe-queries*)
  (every 5000 #(index-directory/refresh-search-managers) (mk-pool) :desc "search refresher")
  (every 1000 #(swap! timer* inc) (mk-pool) :desc "timer")
  (every 1000 #(attempt-gc) (mk-pool) :desc "attempt-gc")
  (discover)
  (every (* 1000 @discover-interval*) #(discover) (mk-pool) :desc "periodic discover")

  (every 10000 #(log/trace "up:" @timer* @index-directory/identifier* @discover-hosts* @peers*) (mk-pool) :desc "dump")
  (repeatedly
   (try
     (run-jetty handler {:port @port*})
     (catch Throwable e
       (do
         (log/warn (ex-str e))
         (Thread/sleep 10000))))))
