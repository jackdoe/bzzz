(ns bzzz.log)
(def level* (atom 1))

(defn fatal [& args]
  (when (>= @level* 0) (println args)))

(defn error [& args]
  (when (>= @level* 0) (println args)))

(defn warn [& args]
  (when (>= @level* 1) (println args)))

(defn info [& args]
  (when (>= @level* 2) (println args)))

(defn debug [& args]
  (when (>= @level* 3) (println args)))

(defn trace [& args]
  (when (>= @level* 4) (println args)))
