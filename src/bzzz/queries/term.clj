(ns bzzz.queries.term
  (:import (org.apache.lucene.search TermQuery)
           (org.apache.lucene.index Term)))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value boost]
         :or {boost 1}} input
         q (TermQuery. (Term. ^String field ^String value))]
    (.setBoost q boost)
    q))
