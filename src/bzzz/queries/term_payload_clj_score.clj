(ns bzzz.queries.term-payload-clj-score
  (use bzzz.expr)
  (:import (org.apache.lucene.search Query IndexSearcher Weight Scorer Explanation ComplexExplanation)
           (org.apache.lucene.index AtomicReaderContext DocsEnum DocsAndPositionsEnum TermsEnum Term)
           (org.apache.lucene.analysis.payloads PayloadHelper)
           (org.apache.lucene.util Bits BytesRef)))

(defn new-scorer [^Term term ^Weight weight ^AtomicReaderContext context ^Bits acceptDocs clj-eval]
  (let [reader (.reader context)]
    (if-let [terms (.terms reader (.field term))]
      (if-let [terms-enum (.iterator terms nil)]
        (if (.seekExact terms-enum (.bytes term))
          (let [postings (.docsAndPositions terms-enum acceptDocs nil DocsAndPositionsEnum/FLAG_PAYLOADS)]
            (proxy [Scorer] [weight]
              (nextDoc []
                (let [n (.nextDoc postings)]
                  (.nextPosition postings)
                  n))
              (advance [target]
                (let [n (.advance postings target)]
                  (.nextPosition postings)
                  n))
              (cost [] (.cost postings))
              (freq [] (.freq postings))
              (docID [] (.docID postings))
              (score [] (clj-eval (PayloadHelper/decodeInt (.bytes (.getPayload postings)) 0)))))
          nil)
        nil)
      nil)))

;; :query {:term-payload-clj-score {:field "name_payload", :value "zzz"
;;                                  :clj-eval "(fn [payload] (+ 10 payload))"}}}))]
(defn term-payload-expr-score-query [^Term term clj-eval-str]
  (let [clj-eval (load-string clj-eval-str)]
    (proxy [Query] []
      (toString [] clj-eval-str)
      (createWeight [^IndexSearcher searcher]
        (let [query this]
          (proxy [Weight][]
            (explain [^AtomicReaderContext reader-ctx doc]
              (let [s ^Scorer (.scorer ^Weight this reader-ctx ^Bits (.getLiveDocs (.reader reader-ctx)))]
                (if (and s
                         (= (.advance s doc) doc))
                  (Explanation. (.score s) (str "clj-eval: " clj-eval-str))
                  (Explanation. 0 "no matching term"))))
            (getValueForNormalization [] 1)
            (getQuery [] query)
            (normalize [norm boost])
            (toString [^String field] "payload-expr-score")
            (scorer ^Scorer [^AtomicReaderContext reader-ctx ^Bits acceptDocs]
              (new-scorer term this reader-ctx acceptDocs clj-eval))))))))


(defn parse
  [generic input analyzer]
  (let [{:keys [field value clj-eval]} input]
    (term-payload-expr-score-query (Term. ^String field ^String value) clj-eval)))
