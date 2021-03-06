(ns bzzz.index-store
  (use bzzz.analyzer)
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.query)
  (use bzzz.index-directory)
  (use bzzz.index-facet-common)
  (use bzzz.index-spatial)
  (:require [bzzz.cached-eval :as cached-eval])
  (:require [bzzz.index-stat :as stat])
  (:require [bzzz.log :as log])
  (:import (java.io StringReader)
           (java.lang OutOfMemoryError)
           (org.apache.lucene.analysis.tokenattributes CharTermAttribute)
           (org.apache.lucene.facet FacetsConfig FacetField)
           (org.apache.lucene.facet.taxonomy.directory DirectoryTaxonomyWriter DirectoryTaxonomyReader)
           (org.apache.lucene.analysis Analyzer)
           (org.apache.lucene.document Document Field Field$Index Field$Store
                                       IntField LongField FloatField DoubleField)
           (org.apache.lucene.index IndexWriter IndexReader Term IndexableField
                                    IndexWriterConfig DirectoryReader FieldInfo)
           (org.apache.lucene.search Query)
           (org.apache.lucene.store NIOFSDirectory Directory)))

(defn text-field [^Field$Store stored ^String key value]
  (Field. key (as-str value)
          stored
          (if (indexed? key)
            (case [(analyzed? key) (norms? key)]
              [true true] Field$Index/ANALYZED
              [true false] Field$Index/ANALYZED_NO_NORMS
              [false true] Field$Index/NOT_ANALYZED
              [false false] Field$Index/NOT_ANALYZED_NO_NORMS)
            Field$Index/NO)))

(defn numeric-field [^Field$Store stored ^String key ^String value]
  (try
    (if (index_integer? key)
      (IntField. key (int-or-parse value) stored)
      (if (index_long? key)
        (LongField. key (long-or-parse value) stored)
        (if (index_float? key)
          (FloatField. key (float-or-parse value) stored)
          (if (index_double? key)
            (DoubleField. key (double-or-parse value) stored)
            (throw (Throwable. "bad numeric field name"))))))
    (catch Exception e
      (let [ex (str "exception parsing numeric field <" key "> value <" value "> exception: " (ex-str e))]
        (throw (Throwable. ex))))))

(defn add-field [^Document document key value]
  (let [str-key (as-str key)
        stored (if (stored? str-key)
                 Field$Store/YES
                 Field$Store/NO)
        generator (if (numeric? str-key)
                    #(numeric-field stored str-key %)
                    #(text-field stored str-key %))]
    (if (vector? value)
      (do
        (if (stored? str-key)
          (.add document (Field. str-key
                                 "__array_identifier__"
                                 Field$Store/YES
                                 Field$Index/NO)))
        (doseq [v value]
          (.add document (generator v))))
      (.add document (generator value)))))


(defn map->document [hmap spatial-strategy]
  (let [document (Document.)]
    (doseq [[key value] hmap]
      (let [key (as-str key)]
        (if (= location-field key)
          (add-location document value spatial-strategy)
          (add-field document key value))))
    document))

(defn add-facet-field-single [^Document doc dim val]
  (.add ^Document doc
        (FacetField. (as-str dim)
                     ^"[Ljava.lang.String;" (into-array String [val]))))

(defn add-facet-field [^Document doc dim val info ^Analyzer analyzer]
  (if (:use-analyzer info)
    (let [stream (.tokenStream analyzer (as-str (:use-analyzer info)) (StringReader. val))
          termAtt (.getAttribute stream CharTermAttribute)]
      (.reset stream)
      (while (.incrementToken stream)
        (add-facet-field-single doc dim (.toString termAtt)))
      (.close stream))
    (add-facet-field-single doc dim val)))

(defn store-on-shard [index documents facets analyzer force-merge]
  (let [t0 (time-ms)]
    (use-writer index
                (parse-analyzer analyzer)
                force-merge
                (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]
                  (let [config (get-facet-config facets)
                        spatial-strategy (new-spatial-strategy)
                        analyzer (.getAnalyzer writer)]
                    (doseq [m documents]
                      (let [doc (map->document m spatial-strategy)]
                        (doseq [[dim f-info] facets]
                          (if-let [f-val ((keyword dim) m)]
                            (add-facet-field doc dim f-val f-info analyzer)))
                        (if (:id m)
                          (.updateDocument writer ^Term (Term. ^String id-field
                                                               (as-str (:id m)))
                                           (.build config taxo doc))
                          (.addDocument writer (.build config taxo doc))))))
                  (stat/update-count index "store-on-shard-documents" (count documents))
                  { index {:done true
                           :took-internal (time-took t0)
                           :attempt-to-write (count documents)}}))))

(defn default-hash-fn [doc]
  (if-let [id (:id doc)]
    (hash id)
    (hash doc)))

(defn store [input]
  (let [{:keys [index documents analyzer
                facets shard alias-set alias-del
                force-merge number-of-shards hash-fn]
         :or {documents [] analyzer nil facets {}
              shard nil alias-set nil alias-del nil
              force-merge 0 number-of-shards nil
              hash-fn nil}} input]
    (if (or alias-set alias-del)
      (update-alias index alias-del alias-set))

    (if (and shard number-of-shards)
      (throw (Throwable. "you can specify only one of <shard> or <number-of-shards>")))

    (if number-of-shards
      (let [compiled-hash-fn (if hash-fn
                               (cached-eval/get-or-eval hash-fn)
                               default-hash-fn)
            futures (into [] (for [n (range number-of-shards)]
                               (future-if
                                (cond-for-future-per-shard input false number-of-shards)
                                (store-on-shard (sharded (resolve-alias index) n)
                                                (filter (fn [doc]
                                                          (= n (mod (compiled-hash-fn doc) number-of-shards)))
                                                        documents)
                                                facets
                                                analyzer
                                                force-merge))))]
        (into [] (for [f futures] (if (future? f) @f f))))
      (store-on-shard (sharded (resolve-alias index) (or shard 0))
                      documents
                      facets
                      analyzer
                      force-merge))))

(defn delete-from-query
  [index input]
  (let [query (parse-query input (parse-analyzer (:analyzer input)))]
    (use-writer-all (resolve-alias index) (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo]
                                            (.deleteDocuments writer
                                                              ^"[Lorg.apache.lucene.search.Query;"
                                                              (into-array Query [query]))))
    {index (.toString query)}))

(defn delete-all [index]
  (use-writer-all (resolve-alias index)
                  (fn [^IndexWriter writer ^DirectoryTaxonomyWriter taxo] (.deleteAll writer))))
