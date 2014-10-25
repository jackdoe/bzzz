(ns bzzz.random-score-query
  (:import (org.apache.lucene.search Query IndexSearcher Weight Scorer Explanation ComplexExplanation)
           (org.apache.lucene.index AtomicReaderContext)
           (org.apache.lucene.util Bits)))
  
(defn random-score-query [^Query subq base]
  (proxy [Query] []
    (toString [] "random")
    (createWeight [^IndexSearcher searcher]
      (let [sub-weight (.createWeight subq searcher)]
        (proxy [Weight][]
          (explain [^AtomicReaderContext ctx doc]
            (let [e (Explanation. 0 "unknown-random-value")
                  sub-explain (.explain sub-weight ctx doc)]
              ;; because of the nature of random-score-query,
              ;; we cant actually reproduce the same score
              ;; if we had deterministic scorer we can just do:
              ;;
              ;; (let [s (.scorer this ctx (.getLiveDocs (.reader ctx)))]
              ;;   (if (= (.advance s doc) doc)
              ;;     (.addDetail e (Explanation. (score s), "random value"))
              ;;     (Explanation. 0 "no matching term")))
              (.addDetail e sub-explain)
              e))
          (getValueForNormalization [] 1)
          (getQuery [] subq)
          (normalize [norm boost])
          (toString [^String field] "random")
          (scorer [^AtomicReaderContext ctx ^Bits acceptDocs]
            (let [sub-scorer (.scorer sub-weight ctx acceptDocs)]
              (proxy [Scorer] [this]
                (nextDoc [] (.nextDoc sub-scorer))
                (advance [] (.advance sub-scorer))
                (cost [] (.cost sub-scorer))
                (freq [] (.freq sub-scorer))
                (docID [] (.docID sub-scorer))
                (score [] (+ base (rand)))))))))))


