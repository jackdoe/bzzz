(ns bzzz.queries.bool
  (:import (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery Query)))
(defn parse
  [generic input analyzer]
  (let [{:keys [must must-not should minimum-should-match boost]
         :or {minimum-should-match 0 should [] must [] must-not [] boost 1}} input
         top ^BooleanQuery (BooleanQuery. true)]
    (doseq [q must]
      (.add top (generic q analyzer) BooleanClause$Occur/MUST))
    (doseq [q must-not]
      (.add top (generic q analyzer) BooleanClause$Occur/MUST_NOT))
    (doseq [q should]
      (.add top (generic q analyzer) BooleanClause$Occur/SHOULD))
    (.setMinimumNumberShouldMatch top minimum-should-match)
    (.setBoost top boost)
    top))
