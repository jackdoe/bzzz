(ns bzzz.queries.no-zero-score
  (:import (bzzz.java.query NoZeroQuery)
           (org.apache.lucene.search Query)))

(defn parse
  [generic input analyzer]
  (let [{:keys [query boost]
         :or {boost 1}} input
         subq ^Query (generic query analyzer)
         q ^Query (NoZeroQuery. subq)]
    (.setBoost q boost)
    q))
