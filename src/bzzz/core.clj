(ns bzzz.core
  (use ring.adapter.jetty)
  (use bzzz.util)
  (use bzzz.const)
  (use [clojure.string :only (split join)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [bzzz.const :as const])
  (:require [bzzz.index :as index])
  (:require [bzzz.analyzer :as analyzer])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [org.httpkit.client :as http-client])
  (:require [clojure.tools.logging :as log])
  (:import (java.net URL))
  (:gen-class :main true))

(def periodic-pool (mk-pool))
(def timer* (atom 0))
(def port* (atom const/default-port))
(def acceptable-discover-time-diff* (atom const/default-acceptable-discover-time-diff))
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
(defn search-remote [hosts input c]
  (let [part (if (or (vector? hosts) (list? hosts)) hosts [hosts])
        is-multi (> (count part) 1)
        args {:timeout (default-to (:timeout input) 1000)
              :as :text
              :body (json/write-str (if is-multi
                                      (assoc input :hosts part)
                                      input))}
        callback (fn [{:keys [status headers body error]}]
                   (if error
                     (async/>!! c {:exception error})
                     (async/>!! c (jr body))))
        resolved (peer-resolve (first part))]
    (log/debug "<" input "> in part <" part "> to resolved <" resolved ">")
    (try
      (if is-multi
        (http-client/put resolved args callback)
        (http-client/get resolved args callback))
      (catch Throwable e
        (async/>!! c {:exception (ex-str e)})))))

(defn limit [input result field-key sort-key]
  (if-let [hits (field-key result)]
    (let [size (default-to (:size input) default-size)
          sorted (sort-by sort-key #(compare %2 %1) hits)]
      (assoc result field-key (if (and  (> (count hits) size)
                                        (default-to (:enforce-limits input) true))
                                (subvec (vec sorted) 0 size)
                                sorted)))
    result))

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

(defn merge-facets [result]
  ;; produces not-sorted output
  (into {}
        (for [[k v] (default-to result {})]
          [k (vals (reduce (fn [sum next]
                             (let [l (:label next)]
                               (if (contains? sum l)
                                 (update-in sum [l :count] + (:count next))
                                 (assoc sum l next))))
                           {}
                           v))])))

(defn merge-and-limit-facets [input result]
  (assoc result
    :facets
    (into {} (for [[k v] (merge-facets (:facets result))]
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

               [k (limit (input-facet-settings input k)
                         v
                         (keyword k)
                         :count)]))))

(defn remote-reducer [sum next]
  (if (:exception next)
    (throw (Throwable. (as-str (:exception next)))))
  (-> sum
      (update-in [:facets] concat-facets (get next :facets))
      (update-in [:total] + (get next :total))
      (update-in [:hits] concat (get next :hits))))

(defn search-many [hosts input]
  (let [c (async/chan)
        ms-start (time-ms)]

    (doseq [part hosts]
      (search-remote part input c))
    (let [collected (into [] (for [part hosts] (async/<!! c)))
          result (reduce remote-reducer
                         { :total 0, :hits [], :took -1 }
                         collected)
          fixed-result (assoc-in (merge-and-limit-facets input result)
                                 [:took]
                                 (time-took ms-start))]
      (limit input fixed-result :hits :_score))))

(defn stat []
  {:index (index/index-stat)
   :analyzer (analyzer/analyzer-stat)
   :identifier @index/identifier*
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
  {:identifier @index/identifier* :discover-hosts @discover-hosts*})

(defn work [method uri input]
  (log/debug "received request" method input)
  (condp = method
    :post (mapply index/store input)
    :delete (index/delete-from-query (:index input)
                                     (:query input))
    :get (case uri
           "/_stat" (stat)
           "/favicon.ico" "" ;; XXX
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
      (let [ex (ex-str e)]
        (log/warn request "->" ex)
        {:status 500
         :headers {"Content-Type" "text/plain"}
         :body ex}))))

(defn update-discovery-state [host c]
  (try
    (log/debug "sending discovery query to" host)
    (http-client/patch host
                       {:body (json/write-str {:discover-hosts @discover-hosts*})
                        :as :text
                        :timeout 500}
                       (fn [{:keys [status headers body error]}]
                         (if error
                           (log/info error)
                           (let [info (jr body)
                                 host-identifier (keyword (need :identifier info "need identifier"))
                                 remote-discover-hosts (:discover-hosts info)]
                             (log/debug "updating" host "with identifier" host-identifier)
                             (locking peers*
                               (swap! peers*
                                      assoc-in [host-identifier host] @timer*))
                             (merge-discover-hosts remote-discover-hosts)))
                         (async/>!! c true)))
    (catch Exception e
      (log/warn "update-discovery-state" host (.getMessage e))
      (async/>!! c false))))

(defn discover []
  ;; just add localhost:port to the possible servers for specific identifier
  ;; hackish, just POC
  (locking peers*
    (swap! peers*
           assoc-in [@index/identifier* (join ":" ["http://localhost" (as-str @port*)]) ] @timer*))
  (let [c (async/chan)
        hosts @discover-hosts*]
    (doseq [[host unused] hosts]
      (update-discovery-state host c))
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
    (reset! index/identifier* (keyword (:identifier options)))
    (reset! index/root* (:directory options))
    (reset! port* (:port options)))

  (index/bootstrap-indexes)
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(index/shutdown)))
  (log/info "starting bzzz --identifier" (as-str @index/identifier*) "--port" @port* "--directory" @index/root* "--hosts" @discover-hosts* "--acceptable-discover-time-diff" @acceptable-discover-time-diff*)
  (every 5000 #(index/refresh-search-managers) periodic-pool :desc "search refresher")
  (every 1000 #(swap! timer* inc) periodic-pool :desc "timer")
  (every 1000 #(discover) periodic-pool :desc "periodic discover")
  (every 10000 #(log/info "up:" @timer* @index/identifier* @discover-hosts* @peers*) periodic-pool :desc "dump")
  (run-jetty handler {:port @port*}))
