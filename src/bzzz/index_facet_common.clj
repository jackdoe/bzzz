(ns bzzz.index-facet-common
  (use bzzz.util)
  (:import (org.apache.lucene.facet FacetsConfig)))

(defn get-facet-config ^FacetsConfig [facets]
  (let [config (FacetsConfig.)]
    (doseq [[dim f-info] facets]
      (.setMultiValued config (as-str dim) true)
      (.setRequireDimCount config (as-str dim) true))
    config))












