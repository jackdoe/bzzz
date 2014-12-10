(ns bzzz.queries.term
  (use bzzz.util)
  (:import (org.apache.lucene.search TermQuery)
           (org.apache.lucene.index Term)))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value boost]
         :or {boost 1}} input
         q (TermQuery. (Term. ^String (need field "need <field>") ^String value))]
    (.setBoost q boost)
    q))
