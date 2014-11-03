(ns bzzz.expr-score-query
  (:import (org.apache.lucene.search Query IndexSearcher Weight Scorer Explanation ComplexExplanation)
           (org.apache.lucene.queries.function ValueSource FunctionValues)
           (org.apache.lucene.expressions Expression SimpleBindings)
           (org.apache.lucene.index AtomicReaderContext)
           (java.util Map)
           (org.apache.lucene.util Bits)))

(defn expr-score-query [^Query subq ^ValueSource vs]
  (proxy [Query] []
    (toString [] "expr-score")
    (createWeight [^IndexSearcher searcher]
      (let [sub-weight (.createWeight subq searcher)
            fcontext (ValueSource/newContext searcher)
            vs-weight (.createWeight vs fcontext searcher)
            new-scorer (fn [^AtomicReaderContext reader-ctx ^Bits acceptDocs]
                         (let [sub-scorer ^Scorer (.scorer sub-weight reader-ctx acceptDocs)]
                           (.put fcontext "scorer" sub-scorer)
                           (if sub-scorer
                             [sub-scorer (.getValues vs fcontext reader-ctx)]
                             [nil nil])))]
        (proxy [Weight][]
          (explain [^AtomicReaderContext reader-ctx doc]
            (let [[^Scorer sub-scorer
                   ^FunctionValues expr-values] (new-scorer reader-ctx (.getLiveDocs (.reader reader-ctx)))
                  sub-explain (.explain sub-weight reader-ctx doc)]
              (if (= (.advance sub-scorer doc) doc)
                (let [top (Explanation. (.doubleVal expr-values (.docID sub-scorer))
                                        (str "overwriting query score with expression's score - " (.toString vs)))]
                  (.addDetail top sub-explain)
                  top)
                (Explanation. 0 "no matching term"))))
          (getValueForNormalization [] (.getValueForNormalization sub-weight))
          (getQuery [] subq)
          (normalize [norm boost] (.normalize sub-weight norm boost))
          (toString [^String field] "expr-weight")
          (scorer [^AtomicReaderContext reader-ctx ^Bits acceptDocs]
            (let [[^Scorer sub-scorer
                   ^FunctionValues expr-values] (new-scorer reader-ctx acceptDocs)]
              (if-not sub-scorer
                nil
                (proxy [Scorer] [this]
                  (nextDoc [] (.nextDoc sub-scorer))
                  (advance [target] (.advance sub-scorer target))
                  (cost [] (.cost sub-scorer))
                  (freq [] (.freq sub-scorer))
                  (docID [] (.docID sub-scorer))
                  (score [] (.doubleVal expr-values (.docID sub-scorer))))))))))))
