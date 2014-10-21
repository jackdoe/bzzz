(ns bzzz.analyzer
  (use bzzz.util)
  (use bzzz.const)
  (:import (java.io StringReader File Reader)
           (java.util.regex Pattern)
           (org.apache.lucene.analysis.pattern PatternReplaceCharFilter)
           (org.apache.lucene.analysis.position PositionFilter)
           (org.apache.lucene.analysis.miscellaneous LengthFilter LimitTokenCountFilter)
           (org.apache.lucene.analysis.reverse ReverseStringFilter)
           (org.apache.lucene.analysis.charfilter BaseCharFilter HTMLStripCharFilter)
           (org.apache.lucene.analysis.core KeywordTokenizer WhitespaceTokenizer LetterTokenizer LowerCaseFilter)
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
;;    filter => [{ type => "lowercase"}],
;;    char-filter => [{ type => "pattern", "pattern" => "X+", "replacement" => "ZZ" }]
;; }
(defn gen-char-filter [^Reader reader obj]
  (let [type (need :type obj "need char-filter type: 'pattern|html-strip..'")]
    (case (as-str type)
      ;; http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html?is-external=true
      ;; https://lucene.apache.org/core/4_9_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceCharFilter.html
      "pattern-replace" (PatternReplaceCharFilter. (Pattern/compile (need :pattern obj "need pattern"))
                                                   (need :replacement obj "need replacement")
                                                   reader)
      "html-strip" (HTMLStripCharFilter. reader (set (default-to (:escaped-tags obj) []))))))

(defn to-char-filter [^Reader reader filters]
  (if (= (count filters) 0)
    reader
    (to-char-filter (gen-char-filter reader (first filters)) (next filters))))

(defn to-lucene-tokenizer [name obj ^Reader reader]
  (let [char-filter (to-char-filter reader
                                    (default-to (:char-filter obj) []))]
    (case (as-str name)
      "whitespace" (WhitespaceTokenizer. *version* char-filter)
      "letter" (LetterTokenizer. *version* char-filter)
      "keyword" (KeywordTokenizer. char-filter)
      "edge-ngram" (EdgeNGramTokenizer. *version*
                                        char-filter
                                        (need :min_gram obj "need min_gram")
                                        (need :max_gram obj "need max_gram"))
      "ngram" (NGramTokenizer. *version*
                               char-filter
                               (need :min_gram obj "need min_gram")
                               (need :max_gram obj "need max_gram")))))

(defn gen-token-filter [^TokenStream source obj]
  (let [type (need :type obj "need tokenfilter type: 'custom|whitespace|keyword..'")]
    (case (as-str type)
      "lowercase" (LowerCaseFilter. *version* source)
      "limit" (LimitTokenCountFilter. source (need :max-token-count obj "need max-token-count"))
      "length" (LengthFilter. (default-to (:enable-position-increment obj) true)
                              source
                              (need :min obj "need min length")
                              (need :max obj "need max length"))
      "position" (PositionFilter. source (default-to (:position-increment obj) 0))
      "reverse" (ReverseStringFilter. *version* source)
      "edge-ngram" (EdgeNGramTokenFilter. *version*
                                          source
                                          (need :min_gram obj "need min_gram")
                                          (need :max_gram obj "need max_gram"))
      "ngram" (NGramTokenFilter. *version*
                                 source
                                 (need :min_gram obj "need min_gram")
                                 (need :max_gram obj "need max_gram")))))


(defn to-lucene-token-filter [source filters]
  (if (= (count filters) 0)
    source
    (to-lucene-token-filter (gen-token-filter source (first filters)) (next filters))))

;; bloody hell this is awesome!
(defn token-filter-chain [obj]
  (let [tokenizer (need :tokenizer obj "need tokenizer: 'ngram|edge-ngram|whitespace|keyword...'")
        filter (default-to (:filter obj) [])]
    (proxy [Analyzer][]
      (createComponents [^String field ^Reader reader]
        (let [t (to-lucene-tokenizer tokenizer obj reader)]
          (if (> (count filter) 0)
            (new Analyzer$TokenStreamComponents t (to-lucene-token-filter t filter))
            (new Analyzer$TokenStreamComponents t)))))))

(defn parse-lucene-analyzer [obj]
  (let [type (need :type obj "need analyzer type: 'custom|whitespace|keyword..'")]
    (case (as-str type)
      "whitespace" (WhitespaceAnalyzer. *version*)
      "keyword" (KeywordAnalyzer.)
      "standard" (StandardAnalyzer. *version*)
      "custom" (token-filter-chain obj))))

(defn parse-analyzer [input]
  (PerFieldAnalyzerWrapper. (WhitespaceAnalyzer. *version*)
                            (into { id-field (KeywordAnalyzer.) }
                                  (for [[key value] input]
                                    { (as-str key) (parse-lucene-analyzer value) }))))

(def analyzer* (atom (parse-analyzer {}))) ;; FIXME - move to top

(defn analyzer-stat []
  { :last-index-analyzer (.toString ^PerFieldAnalyzerWrapper @analyzer*) })
