(ns bzzz.queries.dis-max
  (use bzzz.util)
  (:import (org.apache.lucene.search DisjunctionMaxQuery)))

(defn parse
  [generic input analyzer]
  (let [{:keys [queries tie-breaker boost]
         :or {tie-breaker 0.5 boost 1}} input
         top ^DisjunctionMaxQuery (DisjunctionMaxQuery. (float-or-parse tie-breaker))]
    (doseq [q queries]
      (.add top (generic q analyzer)))
    (.setBoost top boost)
    top))
