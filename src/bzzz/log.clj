(ns bzzz.log)
(def level* (atom 1))
(def lock (Object.))

(defn do-log [min-level args]
  (when (>= @level* min-level)
    (locking lock
      (println "<" min-level ">" args))))

(defn fatal [& args]
  (do-log 0 args))

(defn error [& args]
  (do-log 0 args))

(defn warn [& args]
  (do-log 1 args))

(defn info [& args]
  (do-log 2 args))

(defn debug [& args]
  (do-log 3 args))

(defn trace [& args]
  (do-log 4 args))
