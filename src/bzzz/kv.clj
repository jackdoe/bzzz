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
  (let [{:keys [file-name obj-name clj-eval]
         :or [clj-eval nil obj-name nil]} input
         db (open-db file-name)]

    (when (not (or clj-eval obj-name))
      (throw (Throwable. "need clj-eval or obj-name")))

    (if clj-eval
      (let [expr (get-or-eval clj-eval)]
        (if obj-name
          (expr db (.getHashMap db obj-name))
          (expr db)))
      (.getHashMap db obj-name))))

(defn store [input]
  (let [{:keys [file-name lock-name clj-eval args args-init-expr]} input
        expr (get-or-eval clj-eval)
        lock (get-lock-obj locks* lock-name)
        db (open-db file-name)]
    (locking lock
      (if args
        (if args-init-expr
          (expr db ((get-or-eval args-init-expr) args))
          (expr db args))
        (expr db)))))
