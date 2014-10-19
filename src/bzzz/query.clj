(ns bzzz.query
  (use bzzz.const)
  (use bzzz.util)
  (use bzzz.analyzer)
  (:import (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.index Term)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery IndexSearcher Query ScoreDoc
                                     Scorer TermQuery SearcherManager
                                     Explanation ComplexExplanation
                                     MatchAllDocsQuery
                                     Collector TopScoreDocCollector TopDocsCollector)))
(declare parse-query)
(defn parse-lucene-query-parser
  ^Query
  [& {:keys [query default-field default-operator analyzer]
      :or {default-field "_default_", default-operator "and" analyzer nil}}]
  (let [parser (doto
                   (QueryParser. *version* (as-str default-field) (if (nil? analyzer)
                                                                    @analyzer*
                                                                    (parse-analyzer analyzer)))
                 (.setDefaultOperator (case (as-str default-operator)
                                        "and" QueryParser/AND_OPERATOR
                                        "or"  QueryParser/OR_OPERATOR)))]
    (.parse parser query)))

(defn parse-bool-query
  ^Query
  [& {:keys [must should minimum-should-match boost]
      :or {minimum-should-match 0 should [] must [] boost 1}}]
  (let [top ^BooleanQuery (BooleanQuery. true)]
    (doseq [q must]
      (.add top (parse-query q) BooleanClause$Occur/MUST))
    (doseq [q should]
      (.add top (parse-query q) BooleanClause$Occur/SHOULD))
    (.setMinimumNumberShouldMatch top minimum-should-match)
    (.setBoost top boost)
    top))

(defn parse-term-query
  ^Query
  [& {:keys [field value boost]
      :or {boost 1}}]
  (let [q (TermQuery. (Term. ^String field ^String value))]
    (.setBoost q boost)
    q))

(defn parse-query-fixed ^Query [key val]
  (case (as-str key)
    "query-parser" (mapply parse-lucene-query-parser val)
    "term" (mapply parse-term-query val)
    "match-all" (MatchAllDocsQuery.)
    "bool" (mapply parse-bool-query val)))

(defn parse-query ^Query [input]
  (if (string? input)
    (parse-lucene-query-parser :query input)
    (if (= (count input) 1)
      (let [[key val] (first input)]
        (parse-query-fixed key val))
      (let [top (BooleanQuery. false)]
        (doseq [[key val] input]
          (.add top (parse-query-fixed key val) BooleanClause$Occur/MUST))
        top))))
