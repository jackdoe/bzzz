(ns bzzz.analyzer
  (use bzzz.util)
  (use bzzz.const)
  (:import (java.io StringReader File)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.analysis.miscellaneous PerFieldAnalyzerWrapper)
           (org.apache.lucene.analysis.core WhitespaceAnalyzer KeywordAnalyzer)
           (org.apache.lucene.util Version)))

(defn obj-to-lucene-analyzer [obj]
  (let [name (:use obj)]
    (case (as-str name)
      "whitespace" (WhitespaceAnalyzer. *version*)
      "keyword" (KeywordAnalyzer.)
      "standard" (StandardAnalyzer. *version*))))

(defn parse-analyzer [input]
  (PerFieldAnalyzerWrapper. (WhitespaceAnalyzer. *version*)
                            (into { id-field (KeywordAnalyzer.) }
                                  (for [[key value] input]
                                    { (as-str key) (obj-to-lucene-analyzer value) }))))
(def analyzer* (atom (parse-analyzer {}))) ;; FIXME - move to top

(defn analyzer-stat []
  (.toString @analyzer*))
