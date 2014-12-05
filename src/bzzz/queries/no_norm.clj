(ns bzzz.queries.no-norm
  (:import (bzzz.java.query NoNormQuery)
           (org.apache.lucene.search Query)))

(defn parse
  [generic input analyzer]
  (let [{:keys [query boost]
         :or {boost 1}} input
         subq ^Query (generic query analyzer)
         q ^Query (NoNormQuery. subq)]
    (.setBoost q boost)
    q))
