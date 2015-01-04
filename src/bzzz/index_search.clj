(ns bzzz.index-search
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
  (use bzzz.expr)
  (use bzzz.index-facet-common)
  (use bzzz.index-directory)
  (use bzzz.index-spatial)
  (:require [bzzz.index-stat :as stat])
  (:require [clojure.tools.logging :as log])
  (:import (java.io StringReader)
           (org.apache.lucene.spatial.query SpatialOperation SpatialArgs)
           (org.apache.lucene.expressions.js JavascriptCompiler)
           (org.apache.lucene.expressions Expression SimpleBindings)
           (org.apache.lucene.facet FacetsConfig FacetField FacetsCollector LabelAndValue)
           (org.apache.lucene.facet.taxonomy FastTaxonomyFacetCounts)
           (org.apache.lucene.facet.taxonomy.directory DirectoryTaxonomyReader)
           (org.apache.lucene.analysis Analyzer TokenStream)
           (org.apache.lucene.document Document)
           (org.apache.lucene.search Filter)
           (org.apache.lucene.search.highlight Highlighter QueryScorer
                                               SimpleHTMLFormatter TextFragment)
           (org.apache.lucene.index IndexReader Term IndexableField)
           (org.apache.lucene.search Query ScoreDoc SearcherManager IndexSearcher FieldDoc TopFieldDocs
                                     Explanation Collector TopScoreDocCollector TopDocs
                                     TopDocsCollector MultiCollector TopFieldCollector FieldValueFilter
                                     SortField Sort SortField$Type )))

(defn document->map
  [^Document doc only-fields score highlighter ^Explanation explanation]
  (let [m (into {:_score score}
                (for [^IndexableField f (.getFields doc)]
                  (let [str-name (.name f)
                        name (keyword str-name)]
                    (if (or (nil? only-fields)
                            (name only-fields))
                      (let [values (.getValues doc str-name)]
                        (if (= (count values) 1)
                          [name (first values)]
                          ;; we store fake element in all arrays
                          ;; so if the user stores ["a"], we actually
                          ;; store ["__array_identifier__","a"]
                          ;; and now we just have to return everything,
                          ;; but the first item
                          [name (vec (rest values))]))))))
        highlighted (highlighter m)]
    (conj
     m
     (when explanation (assoc m :_explain (.toString explanation)))
     (when highlighted (assoc m :_highlight highlighted)))))

(defn fragment->map [^TextFragment fragment fidx]
  {:text (.toString fragment)
   :score (.getScore fragment)
   :index fidx
   :frag-num (wall-hack-field (class fragment) :fragNum fragment)
   :text-start-pos (wall-hack-field (class fragment) :textStartPos fragment)
   :text-end-pos (wall-hack-field (class fragment) :textEndPos fragment)})

(defn get-best-fragments [str field highlighter analyzer max-fragments fidx]
  (map #(fragment->map % fidx)
       (.getBestTextFragments ^Highlighter highlighter
                              ^TokenStream (.tokenStream ^Analyzer analyzer
                                                         (as-str field)
                                                         (StringReader. str))
                              ^String str
                              true
                              (int max-fragments))))

(defn make-highlighter
  [^Query query ^IndexSearcher searcher config ^Analyzer analyzer]
  (if config
    (let [indexReader (.getIndexReader searcher)
          scorer (QueryScorer. (.rewrite query indexReader))
          config (merge {:max-fragments 5
                         :pre "<b>"
                         :post "</b>"}
                        config)
          {:keys [fields max-fragments separator fragments-key pre post use-text-fragments]} config
          highlighter (Highlighter. (SimpleHTMLFormatter. pre post) scorer)]
      (fn [m]
        (into {} (for [field fields]
                   [(keyword field)
                    (if-let [value ((keyword field) m)]
                      (flatten (into [] (for [[fidx str] (indexed (if (vector? value) value [value]))]
                                          (vec (get-best-fragments str
                                                                   field
                                                                   highlighter
                                                                   analyzer
                                                                   max-fragments
                                                                   fidx)))))
                      [])]))))
    (constantly nil)))

(defn input->sort [input searcher]
  (let [input (if (vector? input) input [input])]
    (Sort. ^"[Lorg.apache.lucene.search.SortField;"
           (into-array SortField (into [] (for [obj input]
                                            (let [sort (name->sort-field obj)]
                                              (if (= SortField$Type/REWRITEABLE (.getType sort))
                                                (.rewrite sort searcher)
                                                sort))))))))

(defn get-score-collector ^TopDocsCollector [input pq-size searcher]
  (if input
    (TopFieldCollector/create (input->sort input searcher)
                              pq-size
                              true
                              true
                              true
                              true)
    (TopScoreDocCollector/create pq-size true)))

(defn by-score [a b]
  (compare (:_score b) (:_score a)))

(defn by-count [a b]
  (compare (:count b) (:count a)))

(defn compare-array-of-sort-fields [a b]
  (let [aa (first a)
        bb (first b)]
    (if (and (not aa) (not bb))
      0
      (if-not aa
        -1
        (if-not bb
          1
          (let [c (if (get aa :reverse true)
                    (compare (get bb :value nil) (get aa :value nil))
                    (compare (get aa :value nil) (get bb :value nil)))]
            (if (or (not= c 0) (and (not aa) (not bb)))
              c
              (compare-array-of-sort-fields (rest a) (rest b)))))))))

(defn by-sort-fields [a b]
  (let [c (compare-array-of-sort-fields (get a :_sort []) (get b :_sort []))]
    (if (not= c 0)
      c
      (by-score a b))))

(defn enforce-limits? [input]
  (read-boolean-setting input :enforce-limits true))

(defn can-return-partial? [input]
  (read-boolean-setting input :can-return-partial false))

(defn limit [input hits sorter]
  (let [size (get input :size default-size)
        sorted (sort sorter hits)]
    (if (and (> (count hits) size)
             (enforce-limits? input))
      (subvec (vec sorted) 0 size)
      sorted)))

(defn concat-facets [big small]
  (if (not big)
    small ;; initial reduce
    (into big
          (for [[k v] small]
            (if-let [big-list (get big k)]
              [k (concat v big-list)]
              [k v])))))

(defn input-facet-settings [input dim]
  (let [global-ef (enforce-limits? input)
        config (get-in input [:facets (keyword dim)] {})]
    (if (contains? config :enforce-limits)
      config
      (assoc config :enforce-limits global-ef))))

(defn merge-facets [facets]
  ;; produces not-sorted output
  (into {}
        (for [[k v] (default-to facets {})]
          [(keyword k) (vals (reduce (fn [sum next]
                                       (let [l (:label next)]
                                         (if (contains? sum l)
                                           (update-in sum [l :count] + (:count next))
                                           (assoc sum l next))))
                                     {}
                                     v))])))

(defn merge-and-limit-facets [input facets]
  (into {} (for [[k v] (merge-facets facets)]
             ;; this is broken by design
             ;; :__shard_2 {:facets {:name [{:label "jack doe"
             ;;                              :count 100}
             ;;                             {:label "john doe"
             ;;                              :count 10}]}}
             ;;                          ;; -----<cut>-------
             ;;                          ;; {:label "foo bar"
             ;;                          ;; :count 8}
             ;;
             ;; :__shard_3 {:facets {:name [{:label "foo bar"
             ;;                              :count 9}]}}}
             ;;
             ;; so when the multi-search merge happens
             ;; with size=2,it will actully return only
             ;; 'jack doe(100)' and 'john doe(10)' even though
             ;; the actual count of 'foo bar' is 17, because
             ;; __shard_2 actually didnt even send 'foo bar'
             ;; because of the size=2 cut
             [k (limit (input-facet-settings input (keyword k))
                       v
                       by-count)])))

(defn result-reducer [sum next]
  (let [next (if (future? next)
               (try
                 @next
                 (catch Throwable e
                   {:exception (as-str e)}))
               next)
        ex (if (:exception next)
             (if-not (can-return-partial? sum)
               (throw (Throwable. (as-str (:exception next))))
               (do
                 (log/info (str "will send partial response: " (as-str (:exception next))))
                 (as-str (:exception next))))
             nil)]
    (-> sum
        (update-in [:failed] conj-if ex)
        (update-in [:failed] concat-if (:failed next))
        (update-in [:facets] concat-facets (get next :facets {}))
        (update-in [:total] + (get next :total 0))
        (update-in [:hits] concat (get next :hits [])))))

(defn reduce-collection [collection input ms-start]
  (let [result (reduce result-reducer
                       {:total 0
                        :hits []
                        :facets {}
                        :took -1
                        :failed []
                        :can-return-partial (can-return-partial? input)}
                       collection)]
    (-> result
        (assoc-in [:facets] (merge-and-limit-facets input (:facets result)))
        (assoc-in [:hits] (if (:sort input)
                            (limit input (:hits result) by-sort-fields)
                            (limit input (:hits result) by-score)))
        (assoc-in [:took] (time-took ms-start)))))

(defn sorted-fields->map [sort-fields fd-fields]
  (map-indexed (fn [idx ^SortField f]
                 {:reverse (.getReverse f)
                  :name (.getField f)
                  :value (nth fd-fields idx)}) sort-fields))

(defn get-facet-collector-counts [^FastTaxonomyFacetCounts fc facets]
  (into {} (for [[k v] facets]
             (if-let [fr (.getTopChildren fc
                                          (get v :size default-size)
                                          (as-str k)
                                          ^"[Ljava.lang.String;" (into-array
                                                                  String []))]
               [(keyword (.dim fr))
                (into [] (for [^LabelAndValue lv (.labelValues fr)]
                           {:label (.label lv)
                            :count (.value lv)}))]))))

(defn shard-search
  [& {:keys [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader query analyzer
             page size explain highlight facets fields facet-config sort spatial-filter shard]}]
  (let [ms-start (time-ms)
        analyzer ^Analyzer (parse-analyzer analyzer)
        query ^Query (parse-query query analyzer)
        hackish-queries (hack-extract-hackish-queries query)
        highlighter (make-highlighter query searcher highlight analyzer)
        pq-size (+ (* page size) size)
        score-collector (get-score-collector sort pq-size searcher)
        facet-collector (FacetsCollector.)
        spatial-filter (if spatial-filter
                         ^Filter (make-spatial-filter spatial-filter)
                         nil)
        wrap (MultiCollector/wrap
              ^"[Lorg.apache.lucene.search.Collector;"
              (into-array Collector
                          [score-collector
                           facet-collector]))]
    (.search searcher
             query
             spatial-filter
             wrap)
    (stat/update-count shard "shard-search-collect-total" (.getTotalHits score-collector))
    {:total (.getTotalHits score-collector)
     ;; facets:
     ;; do not send the error back,
     ;; for example with no taxo reader, probably problem with open and exception is thrown
     ;; even though we might fake a facet result
     ;; it could really surprise the client
     :facets (merge
              (hack-merge-dynamic-facets-counts hackish-queries)
              (if (and taxo-reader (> (count facets) 0))
                (try
                  (let [fc (FastTaxonomyFacetCounts. taxo-reader
                                                     facet-config
                                                     facet-collector)]
                    (get-facet-collector-counts fc facets))
                  (catch Throwable e
                    (let [ex (ex-str e)]
                      (log/warn (ex-str e))
                      {})))))
     :hits (let [top (.topDocs score-collector (* page size))]
             (into [] (for [^ScoreDoc hit (.scoreDocs top)]
                        (let [doc (hack-merge-result-state hackish-queries
                                                           (.doc hit)
                                                           (document->map (.doc searcher (.doc hit))
                                                                          fields
                                                                          (.score hit)
                                                                          highlighter
                                                                          (when explain
                                                                            (.explain searcher query (.doc hit)))))]
                          (if sort
                            (assoc doc :_sort (sorted-fields->map (.fields ^TopFieldDocs top)
                                                                  (.fields ^FieldDoc hit)))
                            doc)))))
     :took (time-took ms-start)}))

(defn search [input]
  (let [ms-start (time-ms)
        facets (get input :facets)
        index (need :index input "need <index>")
        shards (index-name-matching (resolve-alias index))
        n-shards (count shards)
        futures (into [] (for [shard shards]
                           (future-if
                            (cond-for-future-per-shard input true n-shards)
                            (use-searcher shard
                                          (get input :must-refresh false)
                                          (fn [^IndexSearcher searcher ^DirectoryTaxonomyReader taxo-reader]
                                            (shard-search :searcher searcher
                                                          :shard shard
                                                          :taxo-reader taxo-reader
                                                          :analyzer (get input :analyzer)
                                                          :query (get input :query)
                                                          :facet-config (get-facet-config facets)
                                                          :facets facets
                                                          :highlight (get input :highlight)
                                                          :page (get input :page 0)
                                                          :size (get input :size default-size)
                                                          :sort (get input :sort)
                                                          :spatial-filter (get input :spatial-filter nil)
                                                          :explain (get input :explain false)
                                                          :fields (get input :fields)))))))]
    (reduce-collection futures input ms-start)))
