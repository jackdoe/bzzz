(ns bzzz.discover
  (use bzzz.util)
  (use [clojure.string :only (join)])
  (:require [bzzz.timer :as timer])
  (:require [bzzz.log :as log])
  (:require [bzzz.const :as const])
  (:require [bzzz.timer :as timer])
  (:require [bzzz.index-stat :as index-stat])
  (:require [bzzz.index-directory :as index-directory])
  (:require [org.httpkit.client :as http-client])
  (:require [clojure.core.async :as async])
  (:require [clojure.data.json :as json])
  (:require [clojure.java.io :as io])
  (:import (java.net URL)))

(def acceptable-discover-time-diff* (atom const/default-acceptable-discover-time-diff))
(def next-gc* (atom 0))
(def discover-hosts* (atom {}))
(def gc-interval* (atom 0))
(def peers* (atom {}))

(defn validate-discover-url [x]
  (let [url (URL. ^String (as-str x))]
    (join "://" [(.getProtocol url) (join ":" [(.getHost url) (.getPort url)])])))

(defn rescent? [[host state]]
  ;; default value for missing :update is now
  (< (- @timer/time* (get state :update @timer/time*)) @acceptable-discover-time-diff*))

(defn not-doing-gc? [[host state]]
  (let [diff (- (get state :next-gc-at (+ @timer/time* 100000)) @timer/time*)]
    (not (< (abs diff) 2)))) ;; regardless if we are 2 seconds before gc
                             ;; or 2 seconds after, try to skip this host

(defn possible-hosts [list]
  (let [rescent (filter rescent? list)
        not-doing-gc (filter not-doing-gc? rescent)]
    (if-not (= 0 (count not-doing-gc))
      (first (rand-nth not-doing-gc))
      (do
        (log/debug "found host after ignoring the gcing ones, dump:" list @timer/time* @discover-hosts* @peers*)
        (if (> (count rescent) 0)
          (first (rand-nth rescent))
          nil)))))

(defn peer-resolve [identifier]
  (let [t0 (time-ms)
        resolved (if-let [all-possible ((keyword identifier) @peers*)]
                   (if-let [host (possible-hosts all-possible)]
                     host
                     ;; there is something in the @peers* table, but it is empty/not-rescenn
                     (throw (Throwable. (str "cannot find possible hosts for:" (as-str identifier)))))
                   ;; nothing matches the identifier, in the @peers* table
                   ;; just return it
                   identifier)]
    resolved))

(defn merge-discover-hosts [x]
  (try
    (if x
      (locking discover-hosts*
        (swap! discover-hosts* merge (into {} (for [host (keys x)]
                                                [(validate-discover-url host) true])))))
    (catch Exception e
      (log/warn "merge-discovery-hosts" x (ex-str e))))
  {:identifier @index-directory/identifier*
   :discover-hosts @discover-hosts*
   :next-gc @next-gc*})

(defn attempt-gc []
  (if (<= @next-gc* 0)
    (do
      (let [t0 (time-ms)]
        (System/gc)
        (index-stat/update-took-count index-stat/total "gc" (time-took t0)))
      (let [half (/ @gc-interval* 2)]
        (reset! next-gc* (+ half (int (rand half))))))
    (swap! next-gc* dec)))

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
                                 host-identifier (keyword (need :identifier info "need <identifier>"))
                                 host-next-gc (+ @timer/time* (get info :next-gc 1000000))
                                 remote-discover-hosts (:discover-hosts info)]
                             (log/trace "updating" host "with identifier" host-identifier)
                             (locking peers*
                               (swap! peers*
                                      assoc-in [host-identifier host] {:update @timer/time*
                                                                       :next-gc-at host-next-gc}))
                             (merge-discover-hosts remote-discover-hosts)))
                         (async/>!! c true)))
    (catch Exception e
      (index-stat/update-error index-stat/total "update-discovery")
      (log/trace "update-discovery-state" host (.getMessage e))
      (async/>!! c false))))

(defn periodic [port file]
  (let [assoc-self-idenfifier (fn [current]
                                (assoc-in
                                 current
                                 [@index-directory/identifier*
                                  (join ":" ["http://localhost" (as-str port)])] {:update @timer/time*
                                                                                  :next-gc-at (+ @timer/time* @next-gc*)}))]
    ;; if we are using a peers file, ignore just read the file every second
    ;; and ignore all discovery
    (if file
      (let [updated-peers (if file
                            (try
                              (into {}
                                    (for [[k v]
                                          (json/read-str (slurp (io/file file)) :key-fn str)]
                                      [(keyword k) v]))
                              (catch Exception e
                                (log/error (ex-str e))
                                nil))
                            {})]
        (when updated-peers
          (reset! peers* (assoc-self-idenfifier updated-peers))))
      (do
        (when (> @gc-interval* 0)
          (attempt-gc))

        (locking peers*
          (swap! peers* assoc-self-idenfifier))

        (let [t0 (time-ms)
              c (async/chan)
              hosts @discover-hosts*
              str-state (json/write-str {:discover-hosts @discover-hosts*})]
          (doseq [[host unused] hosts]
            (update-discovery-state host c str-state))
          (doseq [host hosts]
            (async/<!! c))
          (index-stat/update-took-count index-stat/total "discover" (time-took t0)))))))
