(ns bzzz.timer)

(def time* (atom 0))

(defn tick []
  (swap! time* inc))
