(ns bzzz.queries.term-payload-clj-score
  (use bzzz.expr)
  (use bzzz.util)
  (:import (org.apache.lucene.search Query IndexSearcher Weight Scorer Explanation ComplexExplanation)
           (org.apache.lucene.search FieldCache FieldCache$Ints FieldCache$Longs FieldCache$Floats FieldCache$Doubles)
           (org.apache.lucene.index AtomicReaderContext AtomicReader IndexReader DocsEnum DocsAndPositionsEnum TermsEnum Term)
           (org.apache.lucene.analysis.payloads PayloadHelper)
           (org.apache.lucene.util Bits BytesRef)))

(defn field->field-cache ^FieldCache$Ints [^AtomicReader reader name]
  (if (index_long? name)
    (.getLongs FieldCache/DEFAULT reader name false)
    (if (index_integer? name)
      (.getInts FieldCache/DEFAULT reader name false)
      (if (index_float? name)
        (.getFloats FieldCache/DEFAULT reader name false)
        (if (index_double? name)
          (.getDoubles FieldCache/DEFAULT reader name false)
          (throw (Throwable. (str name " can only get field cache for _int|_long|_float|_double"))))))))

(defn fill-field-cache [^AtomicReader reader fc]
  (into {} (for [name fc]
             [(keyword name) (field->field-cache reader name)])))

(defn new-scorer [^Term term ^Weight weight ^AtomicReaderContext context ^Bits acceptDocs clj-eval clj-state field-cache-req]
  (if-let [terms (.terms (.reader context) (.field term))]
    (let [terms-enum (.iterator terms nil)]
      (if (.seekExact terms-enum (.bytes term))
        (let [state (.termState terms-enum)]
          (.seekExact terms-enum (.bytes term) state)
          (if-let [postings (.docsAndPositions terms-enum acceptDocs nil DocsAndPositionsEnum/FLAG_PAYLOADS)]
            (let [fc (fill-field-cache (.reader context) field-cache-req)]
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
                  (let [payload (.getPayload postings)
                        decoded-payload (if payload
                                          (PayloadHelper/decodeInt (.bytes (.getPayload postings)) (.offset payload))
                                          0)]
                    (clj-eval decoded-payload clj-state fc (.docID postings))))))
              (throw (Throwable. (str (.toString term) " was not indexed with payload data")))))
          nil))
    nil))

;; :query {:term-payload-clj-score {:field "name_payload", :value "zzz"
;;                                  :clj-eval "(fn [payload fc doc-id] (+ 10 payload))"}}}))]
(defn term-payload-expr-score-query [^Term term clj-eval-str field-cache-req]
  (let [clj-state (atom {}) ;; we create one query per shard, and we are not using search with executor within the shard
        clj-eval (eval (read-string clj-eval-str))]
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
                  (Explanation. (.score s) (str "clj-eval: " clj-eval-str field-cache-req))
                  (Explanation. 0 "no matching term"))))
            (getValueForNormalization [] 1)
            (getQuery [] query)
            (normalize [norm boost])
            (toString [^String field] "payload-expr-score")
            (scorer ^Scorer [^AtomicReaderContext context ^Bits acceptDocs]
              (new-scorer term this context acceptDocs clj-eval clj-state field-cache-req))))))))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value clj-eval field-cache]
         :or {field-cache []}} input]
    (term-payload-expr-score-query (Term. ^String field ^String value) clj-eval field-cache)))
