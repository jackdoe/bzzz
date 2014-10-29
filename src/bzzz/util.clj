(ns bzzz.util
  (:require [clojure.data.json :as json])
  (use bzzz.const)
  (use [clojure.repl :only (pst)])
  (use [clojure.string :only (split join)]))
(defn as-str ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

(defn mapply [f & args]
  (apply f (apply concat (butlast args) (last args))))

(defn substring? [^String sub ^String st]
  (not= (.indexOf st sub) -1))

(defmacro with-err-str
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*err* s#]
       ~@body
       (str s#))))

(defn slurp-or-default [io default]
  (let [s (slurp io)]
    (if (= (count s) 0)
      default
      s)))

(defn default-to [x val]
  (if (nil? x)
    val
    x))

(defn time-ms []
  (System/currentTimeMillis))

(defn time-took [start]
  (- (time-ms) start))

(defn need [key obj ex]
  (let [found (key obj)]
    (if (not found)
      (throw (Throwable. (if (string? ex)
                           ^String ex
                           (join " " ex)))))
    found))

;; from https://github.com/arohner/clj-wallhack/blob/master/src/wall/hack.clj
(defn wall-hack-field
  "Access to private or protected field. field-name must be something Named

   class - the class where the field is declared
   field-name - Named
   obj - the instance object, or a Class for static fields"
  [class field-name obj]
  (-> class (.getDeclaredField (name field-name))
    (doto (.setAccessible true))
    (.get obj)))

(defn jr [body]
  (json/read-str body :key-fn keyword))

(defn ex-str [e]
  (with-err-str (pst e 36)))

(defn analyzed? [name]
  (if (or (substring? "_not_analyzed" name)
          (= name id-field))
    false
    true))

(defn norms? [name]
  (if (or (substring? "_no_norms" name)
          (= name id-field))
    false
    true))

(defn index_integer? [name]
  (if (substring? "_integer" name)
    true
    false))

(defn index_long? [name]
  (if (substring? "_long" name)
    true
    false))

(defn index_float? [name]
  (if (substring? "_float" name)
    true
    false))

(defn index_double? [name]
  (if (substring? "_double" name)
    true
    false))

(defn stored? [name]
  (if (and (substring? "_no_store" name)
           (not (= name id-field)))
    false
    true))

(defn indexed? [name]
  (if (and (substring? "_no_index" name)
           (not (= name id-field)))
    false
    true))

(defn numeric? [name]
  (or (index_integer? name)
      (index_float? name)
      (index_double? name)
      (index_long? name)))
