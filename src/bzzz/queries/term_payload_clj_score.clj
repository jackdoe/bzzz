(ns bzzz.queries.term-payload-clj-score
  (use bzzz.expr)
  (:import (org.apache.lucene.search Query IndexSearcher Weight Scorer Explanation ComplexExplanation)
           (org.apache.lucene.index AtomicReaderContext IndexReader DocsEnum DocsAndPositionsEnum TermsEnum Term)
           (org.apache.lucene.analysis.payloads PayloadHelper)
           (org.apache.lucene.util Bits BytesRef)))

(defn new-scorer [^Term term ^Weight weight ^AtomicReaderContext context ^Bits acceptDocs clj-eval]
  (if-let [terms (.terms (.reader context) (.field term))]
    (let [terms-enum (.iterator terms nil)]
      (if (.seekExact terms-enum (.bytes term))
        (let [state (.termState terms-enum)]
          (.seekExact terms-enum (.bytes term) state)
          (if-let [postings (.docsAndPositions terms-enum acceptDocs nil DocsAndPositionsEnum/FLAG_PAYLOADS)]
            (proxy [Scorer] [weight]
              (nextDoc []
                (let [n (.nextDoc postings)]
                  (if-not (= DocsEnum/NO_MORE_DOCS n)
                    (.nextPosition postings))
                  n))
              (advance [target]
                (let [n (.advance postings target)]
                  (if-not (= DocsEnum/NO_MORE_DOCS n)
                    (.nextPosition postings))
                  n))
              (cost [] (.cost postings))
              (freq [] (.freq postings))
              (docID [] (.docID postings))
              (score []
                (let [payload (.getPayload postings)]
                  (clj-eval (if payload
                              (PayloadHelper/decodeInt (.bytes (.getPayload postings)) (.offset payload))
                              0)))))
            (throw (Throwable. (str (.toString term) " was not indexed with payload data")))))
          nil))
    nil))

;; :query {:term-payload-clj-score {:field "name_payload", :value "zzz"
;;                                  :clj-eval "(fn [payload] (+ 10 payload))"}}}))]
(defn term-payload-expr-score-query [^Term term clj-eval-str]
  (let [clj-eval (load-string clj-eval-str)]
    (proxy [Query] []
      (toString [] clj-eval-str)
      (createWeight [^IndexSearcher searcher]
        (let [query this]
          (proxy [Weight][]
            (explain [^AtomicReaderContext context doc]
              (let [s ^Scorer (.scorer ^Weight this context ^Bits (.getLiveDocs (.reader context)))]
                (if (and s
                         (not (= doc DocsEnum/NO_MORE_DOCS))
                         (= (.advance s doc) doc))
                  (Explanation. (.score s) (str "clj-eval: " clj-eval-str))
                  (Explanation. 0 "no matching term"))))
            (getValueForNormalization [] 1)
            (getQuery [] query)
            (normalize [norm boost])
            (toString [^String field] "payload-expr-score")
            (scorer ^Scorer [^AtomicReaderContext context ^Bits acceptDocs]
              (new-scorer term this context acceptDocs clj-eval))))))))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value clj-eval]} input]
    (term-payload-expr-score-query (Term. ^String field ^String value) clj-eval)))
