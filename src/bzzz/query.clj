(ns bzzz.query
  (use bzzz.util)
  (use bzzz.analyzer)
  (require clojure.string)
  (require bzzz.queries.match-all)
  (require bzzz.queries.bool)
  (require bzzz.queries.range)
  (require bzzz.queries.random-score)
  (require bzzz.queries.expr-score)
  (require bzzz.queries.term-payload-clj-score)
  (require bzzz.queries.custom-score)
  (require bzzz.queries.constant-score)
  (require bzzz.queries.term)
  (require bzzz.queries.filtered)
  (require bzzz.queries.query-parser)
  (require bzzz.queries.dis-max)
  (require bzzz.queries.wildcard)
  (require bzzz.queries.fuzzy)
  (require bzzz.queries.no-zero-score)
  (require bzzz.queries.no-norm)
  (:import (org.apache.lucene.search Query BooleanQuery BooleanClause$Occur)
           (bzzz.java.query NoZeroQuery NoNormQuery TermPayloadClojureScoreQuery Helper)
           (org.apache.lucene.analysis Analyzer)))

(declare resolve-and-call)
(def unacceptable-method-pattern (re-pattern "[^a-z\\.-]"))
(def allow-unsafe-queries* (atom false))

(def unsafe-queries {"term-payload-clj-score" true})

(defn parse-query ^Query [input ^Analyzer analyzer]
  (if (string? input)
    (resolve-and-call "query-parser" {:query input} analyzer)
    (if (= (count input) 1)
      (let [[key val] (first input)]
        (resolve-and-call key val analyzer))
      (let [top (BooleanQuery. false)]
        (doseq [[key val] input]
          (.add top (resolve-and-call key val analyzer) BooleanClause$Occur/MUST))
        top))))

(defn resolve-and-call [key val ^Analyzer analyzer]
  (let [sanitized (sanitize key unacceptable-method-pattern)
        parse-method (str "bzzz.queries." sanitized "/parse")]
    (if (and (not @allow-unsafe-queries*)
             (get unsafe-queries sanitized))
      (throw (Throwable. (str "request for unsafe query <" sanitized ">, run the server with --allow-unsafe-queries to execute it")))
      (call parse-method parse-query val analyzer))))

(defn hack-extract-hackish-queries [^Query top]
  (Helper/collect_possible_subqueries top nil TermPayloadClojureScoreQuery))

(defn hack-merge-dynamic-facets-counts [queries]
  (reduce (fn [sum ^TermPayloadClojureScoreQuery query]
            (merge sum (bzzz.queries.term-payload-clj-score/fixed-bucket-aggregation-result query)))
          {}
          queries))

(defn hack-merge-result-state [queries doc-id doc]
  (if (> (count queries) 0)
    (assoc doc :_result_state
           (reduce (fn [sum ^TermPayloadClojureScoreQuery query]
                     (if-let [state (bzzz.queries.term-payload-clj-score/extract-result-state query doc-id)]
                       (conj sum state)
                       sum))
                   []
                   queries))
    doc))

(defn hack-share-local-state [queries]
  (when (> (count queries) 0)
    (let [first-query (first queries)]
      (doseq [^TermPayloadClojureScoreQuery query (rest queries)]
        (bzzz.queries.term-payload-clj-score/share-local-state first-query query)))))
