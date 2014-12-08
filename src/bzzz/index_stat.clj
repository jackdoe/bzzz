(ns bzzz.index-stat
  (use bzzz.util)
  (:import (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent ConcurrentHashMap)))

(def state (ConcurrentHashMap.))
(def total "__system_total__")                 

(defn update-raw [index key increment]
  (let [state-key (str (as-str index) "/" key)
        atomic ^AtomicLong (.putIfAbsent ^ConcurrentHashMap state state-key (AtomicLong. increment))]
    (if atomic
      (.getAndAdd atomic increment)
      increment)))

(defn update [index key n]
  (if-not (= total index)
    (update-raw index key n))
  (update-raw total key n))

(defn update-took-count [index key took]
  (update index (str key "-took") took)
  (update index (str key "-count") 1))

(defn update-error [index key]
  (update index (str key "-error") 1))

(defn get-statistics
  ([] (get-statistics total))
  ([^String index]
     (into {} (for [[^String key ^AtomicLong value] state]
                (if-not (= (.indexOf key index) -1)
                  [(clojure.string/replace key #".*/" "") (.get value)])))))

(defn initial-setup []
  (get-statistics))
