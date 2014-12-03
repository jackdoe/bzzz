(ns bzzz.expr
  (use bzzz.util)
  (use bzzz.const)
  (use bzzz.index-spatial)
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.lucene.expressions.js JavascriptCompiler)
           (org.apache.lucene.spatial SpatialStrategy)
           (org.apache.lucene.expressions Expression SimpleBindings)
           (org.apache.lucene.search SortField Sort SortField$Type )))

(declare input->expression-bindings)

(defn sort-reverse? [m]
  (if-let [order (:order m)]
    (case order
      "asc" false
      "desc" true)
    (bool-or-parse (get m :reverse true))))

(defn read-spatial-sort ^SortField [name]
  (let [strategy (new-spatial-strategy)
        point (read-spatial-shape (:point name))
        vs (.makeDistanceValueSource strategy point)]
    (.getSortField vs (sort-reverse? name))))

(defn read-sort-type ^SortField$Type [name]
  (if (= "_score" name)
    SortField$Type/SCORE
    (if (= "_doc" name)
      SortField$Type/DOC
      (if (index_integer? name)
        SortField$Type/INT
        (if (index_long? name)
          SortField$Type/LONG
          (if (index_float? name)
            SortField$Type/FLOAT
            (if (index_double? name)
              SortField$Type/DOUBLE
              SortField$Type/STRING)))))))

(defn read-field-name ^String [name]
  (if (map? name)
    (as-str (need :field name "missing field [{field:...,reverse:true/false}]"))
    (let [name (as-str name)]
      (if (= location-field name)
        (Throwable. (str location-field " has to be a map {field:..., point:... }"))
        name))))

(defn name->sort-field ^SortField [name]
  (if (and (map? name)
           (:expression name))
    (let [[^Expression expr ^SimpleBindings bindings] (input->expression-bindings name)]
      (.getSortField expr bindings (sort-reverse? name)))
    (let [reverse (if (map? name)
                    (sort-reverse? name)
                    true)
          field-name (read-field-name name)]
      (if (= location-field field-name)
        (read-spatial-sort name)
        (SortField. field-name (read-sort-type field-name) ^Boolean reverse)))))

(defn input->expr ^Expression [input]
  (JavascriptCompiler/compile (get input :expression "")))

(defn input->expression-bindings [input]
  (let [expr (input->expr input)
        bindings (SimpleBindings.)]
    (do
      (.add bindings (name->sort-field "_score"))
      (doseq [binding (get input :bindings [])]
        (.add bindings (name->sort-field binding)))
      [expr bindings])))
