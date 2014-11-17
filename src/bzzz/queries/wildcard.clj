(ns bzzz.queries.wildcard
  (:import (org.apache.lucene.search WildcardQuery)
           (org.apache.lucene.index Term)))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value boost]
         :or {boost 1}} input
         q (WildcardQuery. (Term. ^String field ^String value))]
    (.setBoost q boost)
    q))
