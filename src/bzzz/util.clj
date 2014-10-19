(ns bzzz.util)

(defn as-str ^String [x]
  (if (keyword? x)
    (name x)
    (str x)))

(defn mapply [f & args] (apply f (apply concat (butlast args) (last args))))
(defn substring? [^String sub ^String st]
  (not= (.indexOf st sub) -1))

(defmacro with-err-str
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*err* s#]
       ~@body
       (str s#))))
