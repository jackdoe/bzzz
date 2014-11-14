(ns bzzz.queries.query-parser
  (use bzzz.const)
  (use bzzz.util)
  (:import (org.apache.lucene.queryparser.classic QueryParser)))

(defn parse
  [generic input analyzer]
  (let [{:keys [query default-field default-operator boost]
               :or {default-field "_default_", default-operator "and" boost 1}} input
               parser (doto
                          (QueryParser. *version* (as-str default-field) analyzer)
                        (.setDefaultOperator (case (as-str default-operator)
                                               "and" QueryParser/AND_OPERATOR
                                               "or"  QueryParser/OR_OPERATOR)))
        query (.parse parser query)]
    (.setBoost query boost)
    query))
