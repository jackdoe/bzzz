(ns bzzz.queries.term-payload-clj-score
  (use bzzz.util)
  (:import (org.apache.lucene.index Term)
           (org.apache.lucene.search BooleanQuery BooleanClause$Occur MatchAllDocsQuery)
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

(defn share-local-state [^TermPayloadClojureScoreQuery source ^TermPayloadClojureScoreQuery dest]
  (.swap-local-state ^ExpressionContext (.clj_context dest)
                     (.local-state ^ExpressionContext (.clj_context source))))


(defn parse
  [generic input analyzer]
  (let [{:keys [field value tokenize no-zero clj-eval field-cache fixed-bucket-aggregation match-all-if-empty tokenize-occur-should]
         :or {field-cache [] tokenize false tokenize-occur-should false no-zero true fixed-bucket-aggregation nil match-all-if-empty false}} input
         generator (fn [value n cnt]
                     (let [q ^TermPayloadClojureScoreQuery (TermPayloadClojureScoreQuery. (Term. ^String field ^String value)
                                                                                          clj-eval
                                                                                          ^"[Ljava.lang.String;" (into-array String field-cache)
                                                                                          fixed-bucket-aggregation)
                           clj_context ^ExpressionContext (.clj_context q)]
                       (.set_token_position clj_context n cnt)
                       q))]
    (need field "need <field>")
    (need clj-eval "need <clj-eval>")
    (if tokenize
      (let [top (BooleanQuery. false)
            tokens (Helper/tokenize field value analyzer)]
        (if (and match-all-if-empty (= 0 (count tokens)))
          (MatchAllDocsQuery.)
          (do
            (doseq [[index token] (indexed tokens)]
              (.add top (generator token index (count tokens)) (if tokenize-occur-should BooleanClause$Occur/SHOULD BooleanClause$Occur/MUST)))
            (when tokenize-occur-should
              (.setMinimumNumberShouldMatch top 1))
            (if no-zero
              (NoZeroQuery. top)
              top))))
      (generator value 0 1))))
