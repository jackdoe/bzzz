(ns bzzz.util)

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
      (throw (Throwable. ex)))
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
