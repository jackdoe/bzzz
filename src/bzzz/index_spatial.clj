(ns bzzz.index-spatial
  (use bzzz.util)
  (use bzzz.const)
  (:import (com.spatial4j.core.context SpatialContext)
           (com.spatial4j.core.shape Shape)
           (org.apache.lucene.spatial.query SpatialOperation SpatialArgs SpatialArgsParser)
           (org.apache.lucene.spatial.prefix.tree GeohashPrefixTree)
           (org.apache.lucene.spatial.prefix RecursivePrefixTreeStrategy)
           (org.apache.lucene.spatial SpatialStrategy)
           (org.apache.lucene.search Filter)
           (org.apache.lucene.index IndexableField)
           (org.apache.lucene.document StoredField Document)))

(def spatial-context (SpatialContext/GEO))

(defn new-spatial-strategy ^SpatialStrategy []
  (RecursivePrefixTreeStrategy. (GeohashPrefixTree. spatial-context 11) location-field))

(defn read-spatial-shape [value]
  (.readShapeFromWkt ^SpatialContext spatial-context (as-str value)))

(defn add-location [^Document document value ^SpatialStrategy spatial-strategy]
  (let [shape (read-spatial-shape value)]
    (doseq [^IndexableField f (.createIndexableFields spatial-strategy shape)]
      (.add document f))
    (.add document (StoredField. ^String (.getFieldName spatial-strategy)
                                 ^String (.toString ^SpatialContext spatial-context shape)))))

(defn make-spatial-filter ^Filter [v]
  (.makeFilter (new-spatial-strategy)
               ;; "Intersects(ENVELOPE(-10,-8,22,20)) distErrPct=0.025".
               (.parse (SpatialArgsParser.) v spatial-context)))
