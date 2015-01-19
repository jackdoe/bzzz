(ns bzzz.kv
  (use bzzz.util)
  (use bzzz.cached-eval)
  (:require [clojure.java.io :as io])
  (:require [bzzz.index-directory :as index-directory])
  (:import (org.mapdb DBMaker DB)))

(def db* (atom {}))
(def locks* (atom {}))

(defn open-db ^org.mapdb.DB [file-name]
  (let [file-name (str file-name)
        lock (get-lock-obj locks* file-name)]
    (locking lock
      (if-let [db (get @db* file-name)]
        db
        (let [acceptable-name (index-directory/acceptable-index-name file-name)
              path (index-directory/root-identifier-path)
              x (index-directory/try-create-prefix path)
              file (io/file (index-directory/root-identifier-path) acceptable-name)
              db (.make (DBMaker/newFileDB file))]
          (swap! db* assoc-in [file-name] db)
          db)))))

(defn search [input]
  (let [{:keys [file-name obj-name]} input
        db (open-db file-name)]
    (.getHashMap db obj-name)))

(defn store [input]
  (let [{:keys [file-name lock-name clj-eval]} input
        expr (get-or-eval clj-eval)
        lock (get-lock-obj locks* lock-name)
        db (open-db file-name)]
    (locking lock
      (expr db))))
