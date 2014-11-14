(ns bzzz.queries.range
  (use bzzz.util)
  (:import (org.apache.lucene.search NumericRangeQuery)))


(defn parse
  [generic input analyzer]
  (let [{:keys [^String field min max ^Boolean min-inclusive ^Boolean max-inclusive boost]
         :or {min nil max nil min-inclusive true max-inclusive false boost 1}} input
         str-field (as-str field)]
    (if (not (numeric? str-field))
      (throw (Throwable. (str field " is not numeric (need to have _integer|_float|_double|_long in the name"))))
    (let [q (if (index_integer? str-field)
              (NumericRangeQuery/newIntRange str-field
                                             (is-parse-nil min #(int-or-parse %))
                                             (is-parse-nil max #(int-or-parse %))
                                             min-inclusive
                                             max-inclusive)
              (if (index_long? str-field)
                (NumericRangeQuery/newLongRange str-field
                                                (is-parse-nil min #(long-or-parse %))
                                                (is-parse-nil max #(long-or-parse %))
                                                min-inclusive
                                                max-inclusive)

                (if (index_float? str-field)
                  (NumericRangeQuery/newFloatRange str-field
                                                   (is-parse-nil min #(float-or-parse %))
                                                   (is-parse-nil max #(float-or-parse %))
                                                   min-inclusive
                                                   max-inclusive)
                  (NumericRangeQuery/newDoubleRange str-field
                                                    (is-parse-nil min #(double-or-parse %))
                                                    (is-parse-nil max #(double-or-parse %))
                                                    min-inclusive
                                                    max-inclusive))))]
      (.setBoost q boost)
      q)))

