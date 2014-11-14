(ns bzzz.queries.constant-score
  (:import (org.apache.lucene.search ConstantScoreQuery Query)))

(defn parse
  [generic input analyzer]
  (let [{:keys [query boost]
         :or {boost 1}} input
         q (ConstantScoreQuery. ^Query (generic query analyzer))]
    (.setBoost q boost)
    q))
