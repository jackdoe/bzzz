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
  [analyzer & {:keys [query default-field default-operator boost]
               :or {default-field "_default_", default-operator "and" boost 1}}]
  (let [parser (doto
                   (QueryParser. *version* (as-str default-field) analyzer)
                 (.setDefaultOperator (case (as-str default-operator)
                                        "and" QueryParser/AND_OPERATOR
                                        "or"  QueryParser/OR_OPERATOR)))
        query (.parse parser query)]
    (.setBoost query boost)
    query))

(defn parse-bool-query
  ^Query
  [analyzer & {:keys [must should minimum-should-match boost]
               :or {minimum-should-match 0 should [] must [] boost 1}}]
  (let [top ^BooleanQuery (BooleanQuery. true)]
    (doseq [q must]
      (.add top (parse-query q analyzer) BooleanClause$Occur/MUST))
    (doseq [q should]
      (.add top (parse-query q analyzer) BooleanClause$Occur/SHOULD))
    (.setMinimumNumberShouldMatch top minimum-should-match)
    (.setBoost top boost)
    top))

(defn parse-term-query
  ^Query
  [analyzer & {:keys [field value boost]
               :or {boost 1}}]
  (let [q (TermQuery. (Term. ^String field ^String value))]
    (.setBoost q boost)
    q))

(defn parse-query-fixed ^Query [key val analyzer]
  (case (as-str key)
    "query-parser" (mapply parse-lucene-query-parser analyzer val)
    "term" (mapply parse-term-query analyzer val)
    "match-all" (MatchAllDocsQuery.)
    "bool" (mapply parse-bool-query analyzer val)))

(defn extract-analyzer [a]
  (if (nil? a)
    @analyzer*
    (parse-analyzer a)))

(defn parse-query ^Query [input analyzer]
  (if (string? input)
    (mapply parse-lucene-query-parser analyzer {:query input})
    (if (= (count input) 1)
      (let [[key val] (first input)]
        (parse-query-fixed key val analyzer))
      (let [top (BooleanQuery. false)]
        (doseq [[key val] input]
          (.add top (parse-query-fixed key val analyzer) BooleanClause$Occur/MUST))
        top))))
