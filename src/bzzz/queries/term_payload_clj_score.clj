(ns bzzz.queries.term-payload-clj-score
  (use bzzz.util)
  (:import (org.apache.lucene.index Term)
           (bzzz.java.query TermPayloadClojureScoreQuery)))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value clj-eval field-cache]
         :or {field-cache []}} input]
    (need field "need <field>")
    (need clj-eval "need <clj-eval>")
    (TermPayloadClojureScoreQuery. (Term. ^String field ^String value)
                               clj-eval
                               ^"[Ljava.lang.String;" (into-array String field-cache))))
