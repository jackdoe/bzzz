(ns bzzz.expr
  (use bzzz.util)
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.lucene.expressions.js JavascriptCompiler)
           (org.apache.lucene.expressions Expression SimpleBindings)
           (org.apache.lucene.search SortField Sort SortField$Type )))

(declare input->expression-bindings)

(defn sort-reverse? [m]
  (if-let [order (:order m)]
    (case order
      "asc" false
      "desc" true)
    (bool-or-parse (get m :reverse true))))

(defn name->sort-field ^SortField [name]
  (if (and (map? name)
           (:expression name))
    (let [[^Expression expr ^SimpleBindings bindings] (input->expression-bindings name)]
      (.getSortField expr bindings (sort-reverse? name)))
    (let [reverse (if (map? name)
                    (sort-reverse? name)
                    true)
          name (if (map? name)
                 (as-str (need :field name "missing field [{field:...,reverse:true/false}]"))
                 (as-str name))
          type (if (= "_score" name)
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
                           SortField$Type/STRING))))))]
      (SortField. ^String name ^SortField$Type type ^Boolean reverse))))

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
