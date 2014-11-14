(ns bzzz.queries.match-all
  (:import (org.apache.lucene.search MatchAllDocsQuery)))

(defn parse [generic input analyzer]
  (MatchAllDocsQuery.))
