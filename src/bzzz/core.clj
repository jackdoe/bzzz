(ns bzzz.core
  (use ring.adapter.jetty)
  (use bzzz.util)
  (use bzzz.const)
  (use [clojure.repl :only (pst)])
  (use [overtone.at-at :only (every mk-pool)])
  (:require [bzzz.const :as const])
  (:require [bzzz.index :as index])
  (:require [clojure.core.async :as async])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as http-client])
  (:require [clojure.tools.logging :as log])
  (:gen-class :main true))

(set! *warn-on-reflection* true)
(def cron-tp (mk-pool))
(def port* (atom const/default-port))

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
    :post (index/store (:index input) (:documents input) (:analyzer input))
    :delete (index/delete-from-query (:index input) (:query input))
    :get (mapply index/search input)
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

(defn port-validator [port]
  (< 0 port 0x10000))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :id :port
    :default const/default-port
    :parse-fn #(Integer/parseInt %)
    :validate [ #(port-validator %) "Must be a number between 0 and 65536"]]
   ["-d" "--directory DIRECTORY" "directory that will contain all the indexes"
    :id :directory
    :default const/default-root]])

(defn -main [& args]
  (let [{:keys [options errors]} (parse-opts args cli-options)]
    (when (not (nil? errors))
      (log/fatal errors)
      (System/exit 1))
    (log/info options)
    (reset! index/root* (:directory options))
    (reset! port* (:port options)))
  (index/bootstrap-indexes)
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(index/shutdown)))
  (every 5000 #(index/refresh-search-managers) cron-tp :desc "search refresher")
  (log/info "starting bzzzz on port" @port* "with index root directory" @index/root*)
  (run-jetty handler {:port @port*}))
