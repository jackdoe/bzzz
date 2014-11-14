(ns bzzz.query
  (use bzzz.util)
  (use bzzz.analyzer)
  (use [clojure.string :only (replace)])
  (require bzzz.queries.match-all)
  (require bzzz.queries.bool)
  (require bzzz.queries.range)
  (require bzzz.queries.random-score)
  (require bzzz.queries.expr-score)
  (require bzzz.queries.custom-score)
  (require bzzz.queries.constant-score)
  (require bzzz.queries.term)
  (require bzzz.queries.filtered)
  (require bzzz.queries.query-parser)
  (:import (org.apache.lucene.search Query BooleanQuery BooleanClause$Occur)))

(declare resolve-and-call)
(def unacceptable-method-pattern (re-pattern "[^a-z\\.-]"))

(defn extract-analyzer [a]
  (if (nil? a)
    @analyzer*
    (parse-analyzer a)))

(defn parse-query ^Query [input analyzer]
  (if (string? input)
    (resolve-and-call "query-parser" {:query input} analyzer)
    (if (= (count input) 1)
      (let [[key val] (first input)]
        (resolve-and-call key val analyzer))
      (let [top (BooleanQuery. false)]
        (doseq [[key val] input]
          (.add top (resolve-and-call key val analyzer) BooleanClause$Occur/MUST))
        top))))

(defn resolve-and-call [key val analyzer]
  (let [sanitized (replace (as-str key) unacceptable-method-pattern "")
        method (str "bzzz.queries." sanitized "/parse")]
    (call method parse-query val analyzer)))
