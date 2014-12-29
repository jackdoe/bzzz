(ns bzzz.queries.term-payload-clj-score
  (use bzzz.util)
  (:import (org.apache.lucene.index Term)
           (org.apache.lucene.search MatchAllDocsQuery)
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

(defn parse
  [generic input analyzer]
  (let [{:keys [field value tokenize no-zero clj-eval field-cache fixed-bucket-aggregation match-all-if-empty init-state]
         :or {field-cache [] tokenize false no-zero true fixed-bucket-aggregation nil match-all-if-empty false init-state nil}} input]
    (need field "need <field>")
    (need clj-eval "need <clj-eval>")
    (let [tokens (if tokenize
                   (Helper/tokenize field value analyzer)
                   (if (seq? value) value [value]))]
      (if (and match-all-if-empty (= 0 (count tokens)))
        (MatchAllDocsQuery.)
        (let [terms (into [] (for [token tokens]
                               (Term. ^String field ^String token)))
              query (TermPayloadClojureScoreQuery. terms
                                                   clj-eval
                                                   ^"[Ljava.lang.String;" (into-array String field-cache)
                                                   init-state
                                                   fixed-bucket-aggregation)]
          (if no-zero
            (NoZeroQuery. query)
              query))))))
