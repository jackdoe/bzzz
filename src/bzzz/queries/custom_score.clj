(ns bzzz.queries.custom-score
  (use bzzz.expr)
  (:import (org.apache.lucene.queries CustomScoreQuery)
           (org.apache.lucene.search Query)
           (org.apache.lucene.queries.function ValueSource FunctionValues FunctionQuery)
           (org.apache.lucene.expressions Expression SimpleBindings)))


(defn parse [generic input analyzer]
  (let [{:keys [query expression boost]
         :or {query {:match-all {}} boost 1}} input
         [^Expression expr ^SimpleBindings bindings] (input->expression-bindings expression)
         fq ^FunctionQuery (FunctionQuery. (.getValueSource expr bindings))
         q (CustomScoreQuery. ^Query (generic query analyzer) fq)]
    (.setBoost q boost)
    q))
