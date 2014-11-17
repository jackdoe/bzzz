(ns bzzz.queries.fuzzy
  (use bzzz.util)
  (:import (org.apache.lucene.search FuzzyQuery)
           (org.apache.lucene.util.automaton LevenshteinAutomata)
           (org.apache.lucene.index Term)))

(defn parse
  [generic input analyzer]
  (let [{:keys [field value boost max-edits prefix-len max-expansion transpositions]
         :or {boost 1
              max-edits LevenshteinAutomata/MAXIMUM_SUPPORTED_DISTANCE
              max-expansion Integer/MAX_VALUE
              transpositions false
              prefix-len 0}} input
        q (FuzzyQuery. (Term. ^String field ^String value)
                       (int-or-parse max-edits)
                       (int-or-parse prefix-len)
                       (int-or-parse max-expansion)
                       (bool-or-parse transpositions))]
    (.setBoost q boost)
    q))
