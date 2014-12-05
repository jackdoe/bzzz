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
  (:import (org.apache.lucene.search Query BooleanQuery BooleanClause$Occur)))

(declare resolve-and-call)
(def unacceptable-method-pattern (re-pattern "[^a-z\\.-]"))
(def allow-unsafe-queries* (atom false))

(def unsafe-queries {"term-payload-clj-score" true})

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
  (let [sanitized (sanitize key unacceptable-method-pattern)
        parse-method (str "bzzz.queries." sanitized "/parse")]
    (if (and (not @allow-unsafe-queries*)
             (get unsafe-queries sanitized))
      (throw (Throwable. (str "request for unsafe query <" sanitized ">, run the server with --allow-unsafe-queries to execute it")))
      (call parse-method parse-query val analyzer))))
