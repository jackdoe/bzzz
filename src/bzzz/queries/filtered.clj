(ns bzzz.queries.filtered
  (:import (org.apache.lucene.search FilteredQuery QueryWrapperFilter)))

(defn parse
  [generic input analyzer ]
  (let [{:keys [query filter boost]
         :or {boost 1}} input
         q (FilteredQuery. (generic query analyzer)
                           (QueryWrapperFilter. (generic filter analyzer)))]
    (.setBoost q boost)
    q))
