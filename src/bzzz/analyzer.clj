(ns bzzz.analyzer
  (use bzzz.util)
  (use bzzz.const)
  (:import (java.io StringReader File Reader)
           (java.util.regex Pattern)
           (org.apache.lucene.analysis.pattern PatternReplaceCharFilter)
           (org.apache.lucene.analysis.charfilter BaseCharFilter HTMLStripCharFilter)
           (org.apache.lucene.analysis.core KeywordTokenizer WhitespaceTokenizer)
           (org.apache.lucene.analysis Analyzer TokenStream CharFilter Analyzer$TokenStreamComponents)
           (org.apache.lucene.analysis.ngram NGramTokenizer NGramTokenFilter EdgeNGramTokenFilter EdgeNGramTokenizer)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.analysis.miscellaneous PerFieldAnalyzerWrapper)
           (org.apache.lucene.analysis.core WhitespaceAnalyzer KeywordAnalyzer)
           (org.apache.lucene.util Version)))
(set! *warn-on-reflection* true)
;; https://lucene.apache.org/core/4_6_0/core/org/apache/lucene/analysis/Analyzer.html
;; Analyzer analyzer = new Analyzer() {
;;  @Override
;;   protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
;;     Tokenizer source = new FooTokenizer(reader);
;;     TokenStream filter = new FooFilter(source);
;;     filter = new BarFilter(filter);
;;     return new TokenStreamComponents(source, filter);
;;   }
;; };

;; analyzer => { ..
;; field => {
;;    type => custom,
;;    tokenizer => "ngram",
;;    min_gram 3,
;;    max_gram 10,
;;    char-filter => [{ type => "pattern", "pattern" => "X+", "replacement" => "ZZ" }]
;; }
(defn gen-char-filter [^Reader reader obj]
  (if (not (:type obj))
    (throw (Throwable. "need char-filter type: 'pattern|..'")))
  (let [type (:type obj)]
    (case (as-str type)
      ;; http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html?is-external=true
      ;; https://lucene.apache.org/core/4_9_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceCharFilter.html
      "pattern-replace" (PatternReplaceCharFilter. (Pattern/compile (:pattern obj))
                                           (:replacement obj)
                                           reader)
      "html-strip" (HTMLStripCharFilter. reader (set (default-to (:escaped-tags obj) []))))))

(defn to-char-filter
  [^Reader reader filters]
  (if (= (count filters) 0)
    reader
    (to-char-filter (gen-char-filter reader (first filters)) (next filters))))

(defn to-lucene-tokenizer [name obj ^Reader reader]
  (let [char-filter (to-char-filter reader
                                    (default-to (:char-filter obj) []))]
    (case (as-str name)
      "whitespace" (WhitespaceTokenizer. *version* char-filter)
      "keyword" (KeywordTokenizer. char-filter)
      "edge-ngram" (EdgeNGramTokenizer. *version*
                                        char-filter
                                        (default-to (:min_gram obj) 1)
                                        (default-to (:max_gram obj) 30))
      "ngram" (NGramTokenizer. *version*
                               char-filter
                               (default-to (:min_gram obj) 1)
                               (default-to (:max_gram obj) 30)))))

(defn to-lucene-token-filter [tokenizer obj]
  nil)

;; bloody hell this is awesome!
(defn token-filter-chain [obj]
  (let [tokenizer (:tokenizer obj)
        filter (default-to (:filter obj) [])]
    (proxy [Analyzer][]
      (createComponents [^String field ^Reader reader]
        (let [t (to-lucene-tokenizer tokenizer obj reader)
              f (to-lucene-token-filter t filter)]
          (if f
            (new Analyzer$TokenStreamComponents t f)
            (new Analyzer$TokenStreamComponents t)))))))

(defn obj-to-lucene-analyzer [obj]
  (if (not (:type obj))
    (throw (Throwable. "need analyzer type: 'custom|whitespace|keyword..'")))
  (let [type (:type obj)]
    (case (as-str type)
      "whitespace" (WhitespaceAnalyzer. *version*)
      "keyword" (KeywordAnalyzer.)
      "standard" (StandardAnalyzer. *version*)
      "custom" (token-filter-chain obj))))

(defn parse-analyzer [input]
  (PerFieldAnalyzerWrapper. (WhitespaceAnalyzer. *version*)
                            (into { id-field (KeywordAnalyzer.) }
                                  (for [[key value] input]
                                    { (as-str key) (obj-to-lucene-analyzer value) }))))

(def analyzer* (atom (parse-analyzer {}))) ;; FIXME - move to top

(defn analyzer-stat []
  (.toString ^PerFieldAnalyzerWrapper @analyzer*))
