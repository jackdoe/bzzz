(ns bzzz.random-score-query
  (:import (org.apache.lucene.search Query IndexSearcher Weight Scorer)
           (org.apache.lucene.index AtomicReaderContext)
           (org.apache.lucene.util Bits)))
  
(defn random-score-query [^Query subq base]
  (proxy [Query] []
    (toString [] "random")
    (createWeight [^IndexSearcher searcher]
      (let [sub-weight (.createWeight subq searcher)]
        (proxy [Weight][]
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


