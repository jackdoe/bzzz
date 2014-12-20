(ns bzzz.queries.term-payload-clj-score
  (use bzzz.util)
  (:import (org.apache.lucene.index Term)
           (bzzz.java.query TermPayloadClojureScoreQuery ExpressionContext)))

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
  (let [{:keys [field value clj-eval field-cache fixed-bucket-aggregation]
         :or {field-cache [] fixed-bucket-aggregation nil}} input]
    (need field "need <field>")
    (need clj-eval "need <clj-eval>")
    (TermPayloadClojureScoreQuery. (Term. ^String field ^String value)
                                   clj-eval
                                   ^"[Ljava.lang.String;" (into-array String field-cache)
                                   fixed-bucket-aggregation)))
