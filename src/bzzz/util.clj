(ns bzzz.util)

(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
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
      (throw (Throwable. ex)))
    found))
