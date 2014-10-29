(ns bzzz.query
  (use bzzz.const)
  (use bzzz.util)
  (use bzzz.analyzer)
  (use bzzz.random-score-query)
  (:import (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.index Term)
           (org.apache.lucene.search BooleanClause BooleanClause$Occur
                                     BooleanQuery IndexSearcher Query ScoreDoc
                                     Scorer TermQuery SearcherManager
                                     Explanation ComplexExplanation
                                     NumericRangeQuery
                                     MatchAllDocsQuery
                                     FilteredQuery QueryWrapperFilter
                                     ConstantScoreQuery
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

(defn parse-filtered-query
  ^Query
  [analyzer & {:keys [query filter boost]
               :or {boost 1}}]
  (let [q (FilteredQuery. (parse-query query analyzer)
                          (QueryWrapperFilter. (parse-query filter analyzer)))]
    (.setBoost q boost)
    q))

(defn parse-random-score-query
  ^Query
  [analyzer & {:keys [query base]
               :or {base 100}}]
  (random-score-query (parse-query query analyzer) base))

(defn parse-constant-score-query
  ^Query
  [analyzer & {:keys [query boost]
               :or {boost 1}}]
  (let [q (ConstantScoreQuery. ^Query (parse-query query analyzer))]
    (.setBoost q boost)
    q))

(defn parse-bool-query
  ^Query
  [analyzer & {:keys [must must-not should minimum-should-match boost]
               :or {minimum-should-match 0 should [] must [] must-not [] boost 1}}]
  (let [top ^BooleanQuery (BooleanQuery. true)]
    (doseq [q must]
      (.add top (parse-query q analyzer) BooleanClause$Occur/MUST))
    (doseq [q must-not]
      (.add top (parse-query q analyzer) BooleanClause$Occur/MUST_NOT))
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

(defn is-parse-nil [x is cast parser]
  (if x
    (if (is x)
      (cast x)
      (parser x))
    nil))

(defn parse-numeric-range-query
  ^Query
  [analyzer & {:keys [^String field min max ^Boolean min-inclusive ^Boolean max-inclusive boost]
               :or {min nil max nil min-inclusive true max-inclusive false boost 1}}]
  (if (not (numeric? field))
    (throw (Throwable. (str field " is not numeric (need to have _integer|_float|_double|_long in the name"))))
  (let [q (if (index_integer? field)
            (NumericRangeQuery/newIntRange field
                                           (is-parse-nil min #(integer? %) #(int %) #(Integer/parseInt %))
                                           (is-parse-nil max #(integer? %) #(int %) #(Integer/parseInt %))
                                           min-inclusive
                                           max-inclusive)
            (if (index_long? field)
              (NumericRangeQuery/newLongRange field
                                              (is-parse-nil min #(integer? %) #(long %) #(Long/parseLong %))
                                              (is-parse-nil max #(integer? %) #(long %) #(Long/parseLong %))
                                              min-inclusive
                                              max-inclusive)

              (if (index_float? field)
                (NumericRangeQuery/newFloatRange field
                                                 (is-parse-nil min #(float? %) #(float %) #(Float/parseFloat %))
                                                 (is-parse-nil max #(float? %) #(float %) #(Float/parseFloat %))
                                                 min-inclusive
                                                 max-inclusive)
                (NumericRangeQuery/newDoubleRange field
                                                  (is-parse-nil min #(float? %) #(double %) #(Double/parseDouble %))
                                                  (is-parse-nil max #(float? %) #(double %) #(Double/parseDouble %))
                                                  min-inclusive
                                                  max-inclusive))))]
    (.setBoost q boost)
    q))

(defn parse-query-fixed ^Query [key val analyzer]
  (case (as-str key)
    "query-parser" (mapply parse-lucene-query-parser analyzer val)
    "term" (mapply parse-term-query analyzer val)
    "filtered" (mapply parse-filtered-query analyzer val)
    "constant-score" (mapply parse-constant-score-query analyzer val)
    "random-score" (mapply parse-random-score-query analyzer val)
    "range" (mapply parse-numeric-range-query analyzer val)
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
