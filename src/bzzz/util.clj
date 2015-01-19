(ns bzzz.util
  (:require [clojure.data.json :as json])
  (:import (java.io File))
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
  (if-not io
    default
    (let [s (slurp io)]
      (if (= (count s) 0)
        default
        s))))

(defn default-to [x val]
  (if (nil? x)
    val
    x))

(defn time-ms []
  (System/currentTimeMillis))

(defn time-took [start]
  (- (time-ms) start))

(defn need
  ([x ex]
     (if (not x)
       (throw (Throwable. (if (string? ex)
                            ^String ex
                            (join " " ex)))))
     x)
  ([key obj ex]
     (need (get obj key) ex)))

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
  (if (or
       (substring? "_integer" name)
       (substring? "_int" name))
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

(defn is-parse-nil [x parser]
  (if x
    (parser x)
    nil))

(defn int-or-parse ^Integer [x]
  (if (instance? Integer x)
    x
    (if (number? x)
      (int x)
      (Integer/parseInt x))))

(defn long-or-parse ^Long [x]
  (if (instance? Long x)
    x
    (if (number? x)
      (long x)
      (Long/parseLong x))))

(defn double-or-parse ^Double [x]
  (if (instance? Double x)
    x
    (if (number? x)
      (double x)
      (Double/parseDouble x))))

(defn float-or-parse ^Float [x]
  (if (instance? Float x)
    x
    (if (number? x)
      (float x)
      (Float/parseFloat x))))

(defn bool-or-parse ^Boolean [x]
  (if (instance? Boolean x)
    x
    (Boolean/parseBoolean x)))

(defn read-boolean-setting [input key default]
  (let [ef (get input key default)]
    (if (and ef (bool-or-parse ef))
      true
      false)))

(defn indexed [coll] (map-indexed vector coll))

(defn delete-recursively [fname]
  (let [func (fn [func ^File f]
               (when (.isDirectory f)
                 (doseq [f2 (.listFiles f)]
                   (func func f2)))
               (clojure.java.io/delete-file f))]
    (func func (clojure.java.io/file fname))))

(defn concat-if [to next]
  (if next
    (concat to next)
    to))

(defn conj-if [to next]
  (if next
    (conj to next)
    to))

(defn abs "(abs n) is the absolute value of n" [n]
  (cond
   (not (number? n)) (throw (IllegalArgumentException.
                             "abs requires a number"))
   (neg? n) (- n)
   :else n))

(defn call [^String nm & args]
  (if-let [fun (ns-resolve *ns* (symbol nm))]
    (apply fun args)
    (throw (IllegalArgumentException. (str "unable to resolve " nm)))))

(defn sanitize ^String [s unacceptable-pattern]
  (clojure.string/replace (as-str s) unacceptable-pattern ""))

(defmacro future-if
  [condition & body]
  `(if ~condition
     (future ~@body)
     ~@body))

(defn cond-for-future-per-shard [input default n-shards]
  (and (read-boolean-setting input :future default) (> n-shards 1)))

; Ref: https://github.com/puppetlabs/clj-kitchensink/commit/32a1a3a98ad08d521e58ccf88062eb7209992973
(defn deep-merge
  "Deeply merges maps so that nested maps are combined rather than replaced.

  For example:
  (deep-merge {:foo {:bar :baz}} {:foo {:fuzz :buzz}})
  ;;=> {:foo {:bar :baz, :fuzz :buzz}}"
  [& vs]
  (if (every? map? vs)
    (apply merge-with deep-merge vs)
    (last vs)))

(defn get-lock-obj [container* key]
  (if-let [obj (get @container* key)]
    obj
    (locking container*
      (if-let [obj (get @container* key)]
        obj
        (let [obj (Object.)]
          (swap! container* assoc key obj)
          obj)))))
