(ns bzzz.core
  (use ring.adapter.jetty)
  (use bzzz.util)
  (use bzzz.const)
  (use [clojure.string :only (split join)])
  (use [clojure.repl :only (pst)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [bzzz.const :as const])
  (:require [bzzz.index :as index])
  (:require [bzzz.analyzer :as analyzer])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as http-client])
  (:require [clojure.tools.logging :as log])
  (:import (java.net URL))
  (:gen-class :main true))

(set! *warn-on-reflection* true)
(def periodic-pool (mk-pool))
(def timer* (atom 0))
(def port* (atom const/default-port))
(def acceptable-discover-time-diff* (atom const/default-acceptable-discover-time-diff))
(def identifier* (atom const/default-identifier))
(def discover-hosts* (atom {}))
(def peers* (atom {}))

(defn peer-resolve [identifier]
  (locking peers*
    (let [all-possible ((keyword identifier) @peers*)]
      (if all-possible
        (let [possible (filter #(< (- @timer* (second %))
                                   @acceptable-discover-time-diff*) all-possible)]
          (if (= (count possible) 0)
            (throw (Throwable. (as-str identifier)))
            (first (rand-nth possible))))
        identifier)))) ;; nothing matches the identifier

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
    (try
      (let [resolved (peer-resolve (first part))]
        (if (> (count part) 1)
          (:body (http-client/put resolved args))
          (:body (http-client/get resolved args))))
      (catch Throwable e
        {:exception (with-err-str (pst e 36))}))))

(defn search-many [hosts input]
  (let [c (async/chan)
        ms-start (time-ms)]
    (doseq [part hosts]
      (async/go (async/>! c (search-remote part input))))
    (let [ collected (into [] (for [part hosts] (async/<!! c)))]
      (reduce (fn [sum next]
                (if (:exception next)
                  (throw (Throwable. (as-str (:exception next)))))
                (-> sum
                    (assoc-in [:took] (time-took ms-start))
                    (update-in [:total] + (:total next))
                    (update-in [:hits] concat (:hits next))))
              { :total 0, :hits [], :took -1 }
              collected))))

(defn stat []
  {:index (index/index-stat)
   :analyzer (analyzer/analyzer-stat)
   :identifier @identifier*
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
  {:identifier @identifier* :discover-hosts @discover-hosts*})

(defn work [method uri input]
  (log/debug "received request" method input)
  (condp = method
    :post (index/store (:index input)
                       (:documents input)
                       (:analyzer input))
    :delete (index/delete-from-query (:index input)
                                     (:query input))
    :get (if (= "/_stat" uri)
           (stat)
           (mapply index/search input))
    :put (search-many (:hosts input) (dissoc input :hosts))
    :patch (merge-discover-hosts (default-to (:discover-hosts input) {}))
    (throw (Throwable. "unexpected method" method))))

(defn handler [request]
  (try
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/write-str (work (:request-method request)
                                 (:uri request)
                                 (json/read-str (slurp-or-default (:body request) "{}") :key-fn keyword)))}
    (catch Throwable e
      (let [ex (with-err-str (pst e 36))]
        (log/warn request "->" ex)
        {:status 500
         :headers {"Content-Type" "text/plain"}
         :body ex}))))

(defn update-discover-state [host]
  (try
    (log/debug "sending discovery query to" host)
    (let [info (:body (http-client/patch host {:accept :json
                                               :as :json
                                               :body (json/write-str {:discover-hosts @discover-hosts*})
                                               :body-encoding "UTF-8"
                                               :socket-timeout 500
                                               :conn-timeout 500}))
          host-identifier (keyword (need :identifier info "need identifier"))
          remote-discover-hosts (:discover-hosts info)]
      (log/debug "updating" host "with identifier" host-identifier)
      (locking peers*
        (swap! peers*
               assoc-in [host-identifier host] @timer*))
      (merge-discover-hosts remote-discover-hosts)
      true)
    (catch Exception e
      (log/warn "update-discovery-state" host (.getMessage e))
      true)))

(defn discover []
  ;; just add localhost:port to the possible servers for specific identifier
  ;; hackish, just POC
  (locking peers*
    (swap! peers*
           assoc-in [@identifier* (join ":" ["http://localhost" (as-str @port*)]) ] @timer*))

  (let [c (async/chan) hosts @discover-hosts*]
    (doseq [[host unused] hosts]
      (async/go (async/>! c (update-discover-state host))))
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
   ["-x" "--accept NUM-IN-SECONDS" "only consider discovered hosts who refreshed witihin the last X seconds"
    :id :acceptable-discover-time-diff
    :default const/default-acceptable-discover-time-diff
    :parse-fn #(Integer/parseInt %)
    :validate [ #(> % 0) "Must be a number > 0"]]
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
    (log/info options)

    ;; split uses java.util.regex.Pattern/split so
    ;; bzzz.core=> (split "" #",")
    ;;[""]
    (merge-discover-hosts (into {} (for [host (filter #(> (count %) 0)
                                                      (split (:discover-hosts options) #","))]
                                     [host true])))
    (reset! acceptable-discover-time-diff* (:acceptable-discover-time-diff options))
    (reset! identifier* (keyword (:identifier options)))
    (reset! index/root* (:directory options))
    (reset! port* (:port options)))

  (index/bootstrap-indexes)
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(index/shutdown)))
  (log/info "starting bzzz[" @identifier* "] on port" @port* "with index root directory" @index/root* "with discover hosts" @discover-hosts* "acceptable-discover-time-diff: " @acceptable-discover-time-diff*)
  (every 5000 #(index/refresh-search-managers) periodic-pool :desc "search refresher")
  (every 1000 #(swap! timer* inc) periodic-pool :desc "timer")
  (every 1000 #(discover) periodic-pool :desc "periodic discover")
  (every 10000 #(log/info @timer* @identifier* @discover-hosts* @peers*) periodic-pool :desc "dump")
  (run-jetty handler {:port @port*}))
