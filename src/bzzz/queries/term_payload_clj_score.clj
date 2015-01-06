(ns bzzz.queries.term-payload-clj-score
  (use bzzz.util)
  (:require [bzzz.index-stat :as index-stat])
  (:import (org.apache.lucene.index Term)
           (org.apache.lucene.search MatchAllDocsQuery)
           (com.googlecode.concurrentlinkedhashmap ConcurrentLinkedHashMap ConcurrentLinkedHashMap$Builder)
           (bzzz.java.query TermPayloadClojureScoreQuery NoZeroQuery ExpressionContext Helper)))

(defn fixed-bucket-aggregation-result [^TermPayloadClojureScoreQuery query]
  (let [fba (.fba_get_results ^ExpressionContext (.clj_context query))]
    (zipmap (.keySet fba)
            (map (fn [f]
                   (map (fn [^java.util.HashMap v]
                          {:count (.get v "count")
                           :label (.get v "label")})
                        (into [] f)))
                 (.values fba)))))

(defn extract-result-state [^TermPayloadClojureScoreQuery query doc-id]
  (.result_state_get_for_doc ^ExpressionContext (.clj_context query) doc-id))

(def expr-cache ^java.util.Map
  (let [b ^java.util.Map (ConcurrentLinkedHashMap$Builder.)]
    (.maximumWeightedCapacity b 1000)
    (.build b)))

(defn get-or-eval [expr]
  (if-let [v (.get ^java.util.Map expr-cache expr)]
    v
    (let [t0 (time-ms)
          evaluated-expr (eval (read-string expr))]
      (index-stat/update-took-count index-stat/total
                                    "eval"
                                    (time-took t0))
      (.put ^java.util.Map expr-cache expr evaluated-expr)
      evaluated-expr)))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value tokenize no-zero clj-eval field-cache fixed-bucket-aggregation match-all-if-empty init-clj-eval]
         :or {field-cache [] tokenize false no-zero true fixed-bucket-aggregation nil match-all-if-empty false init-expr nil}} input]
    (need field "need <field>")
    (need clj-eval "need <clj-eval>")
    (when init-clj-eval
      ((get-or-eval init-clj-eval)))

    (let [tokens (if tokenize
                   (Helper/tokenize field value analyzer)
                   (if (seq? value) value [value]))
          expr (get-or-eval clj-eval)]
      (if (and match-all-if-empty (= 0 (count tokens)))
        (MatchAllDocsQuery.)
        (let [terms (into [] (for [token tokens]
                               (Term. ^String field ^String token)))
              query (TermPayloadClojureScoreQuery. terms
                                                   expr
                                                   ^"[Ljava.lang.String;" (into-array String field-cache)
                                                   fixed-bucket-aggregation)]
          (if no-zero
            (NoZeroQuery. query)
              query))))))
