(ns bzzz.core
  (use ring.adapter.jetty)
  (use bzzz.util)
  (use bzzz.const)
  (use [clojure.string :only (split join)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [bzzz.const :as const])
  (:require [bzzz.index-search :as index-search])
  (:require [bzzz.index-directory :as index-directory])
  (:require [bzzz.index-store :as index-store])
  (:require [bzzz.analyzer :as analyzer])
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
(def discover-hosts* (atom {}))
(def peers* (atom {}))

(defn rescent? [than]
  (< (- @timer* (second than)) @acceptable-discover-time-diff*))

(defn possible-hosts [identifier]
  (filter rescent? (get @peers* (keyword identifier) [])))

(defn peer-resolve [identifier]
  (if-let [all-possible ((keyword identifier) @peers*)]
    (let [possible (filter rescent? all-possible)]
      (if (= (count possible) 0)
        (throw (Throwable. (str "cannot find possible hosts for:" (as-str identifier))))
        (first (rand-nth possible))))
    identifier)) ;; nothing matches the identifier

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
   :timer @timer*})

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
  {:identifier @index-directory/identifier* :discover-hosts @discover-hosts*})

(defn work [method uri qs input]
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
    (throw (Throwable. "unexpected method" method))))

(defn handler [request]
  (try
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/write-str (work (:request-method request)
                                 (:uri request)
                                 (:query-string request)
                                 (json/read-str (slurp-or-default (:body request) "{}") :key-fn keyword)))}
    (catch Throwable e
      (let [ex (ex-str e)]
        (log/warn request "->" ex)
        {:status 500
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:exception ex})}))))

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
                                 remote-discover-hosts (:discover-hosts info)]
                             (log/trace "updating" host "with identifier" host-identifier)
                             (locking peers*
                               (swap! peers*
                                      assoc-in [host-identifier host] @timer*))
                             (merge-discover-hosts remote-discover-hosts)))
                         (async/>!! c true)))
    (catch Exception e
      (log/trace "update-discovery-state" host (.getMessage e))
      (async/>!! c false))))

(defn discover []
  ;; just add localhost:port to the possible servers for specific identifier
  ;; hackish, just POC
  (locking peers*
    (swap! peers*
           assoc-in [@index-directory/identifier* (join ":" ["http://localhost" (as-str @port*)]) ] @timer*))
  (let [c (async/chan)
        hosts @discover-hosts*
        str-state (json/write-str {:discover-hosts @discover-hosts*})]
    (doseq [[host unused] hosts]
      (update-discovery-state host c str-state))
    (doseq [host hosts]
      (async/<!! c))))

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
   ["-r" "--discover-interval NUM-IN-SECONDS" "exchange information with the discover hosts every N seconds"
    :id :discover-interval
    :default const/default-discover-interval
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
    (reset! index-directory/identifier* (keyword (:identifier options)))
    (reset! index-directory/root* (:directory options))
    (reset! port* (:port options)))
  (index-directory/initial-read-alias-file)
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(index-directory/shutdown)))
  (log/info "starting bzzz --identifier" (as-str @index-directory/identifier*) "--port" @port* "--directory" @index-directory/root* "--hosts" @discover-hosts* "--acceptable-discover-time-diff" @acceptable-discover-time-diff* "--discover-interval" @discover-interval*)
  (every 5000 #(index-directory/refresh-search-managers) (mk-pool) :desc "search refresher")
  (every 1000 #(swap! timer* inc) (mk-pool) :desc "timer")
  (every @discover-interval* #(discover) (mk-pool) :desc "periodic discover")
  (every 10000 #(log/trace "up:" @timer* @index-directory/identifier* @discover-hosts* @peers*) (mk-pool) :desc "dump")
  (repeatedly
   (try
     (run-jetty handler {:port @port*})
     (catch Throwable e
       (do
         (log/warn (ex-str e))
         (Thread/sleep 10000))))))
