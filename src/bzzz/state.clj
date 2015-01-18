(ns bzzz.state
  (use bzzz.util)
  (use [clojure.set :only (rename-keys)])
  (:import (bzzz.java.query TermPayloadClojureScoreQuery)))

;; TODO Maybe add support for @hosts?
;; FIXME Deep-merges don't seem very useful anymore. Drop 'em?

(def temporary-state (atom {}))

(defn ro-merge
  "Merge the given data map into the global read-only state.

  This is useful for one-shot updates, where the whole state can
  safely fit into a request.

  Example:
  (ro-merge { :small-table [...] })

  The behavior is exactly the same as clojure.core/merge"
  [data]
  (TermPayloadClojureScoreQuery/replace_expr_global_state_ro
   (merge TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO data)))

(defn ro-deep-merge
  "Deeply merge given data map into the global read-only state.

  This is useful for incremental updates, where you can't (or shouldn't)
  send the whole state in the same request.

  Example:
  (ro-deep-merge { :dataset { :foo 1 }})
  (ro-deep-merge { :dataset { :bar 2 }})

  Refer to bzzz.util/deep-merge for more details"
  [data]
  (TermPayloadClojureScoreQuery/replace_expr_global_state_ro
    (deep-merge TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO data)))

(defn ro-rename-key
  "Rename a top-level key in the global read-only state.

  This can be used for scenarios where having an outdated state is
  preferable over having a partially updated one. Typically coupled
  with bzzz.state/ro-deep-merge.

  Example:
  (ro-deep-merge { :tmp-dataset { :foo 3 }})
  (ro-deep-merge { :tmp-dataset { :bar 5 }})
  (ro-rename :tmp-dataset :dataset)"
  [from_key to_key]
  (TermPayloadClojureScoreQuery/replace_expr_global_state_ro
    (rename-keys TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO
                 { (keyword from_key) (keyword to_key) })))

(defn temp-assoc-in
  "Associates input data into the temporary map.

  Example:
  (temp-assoc-in [ [[:foo :bar] 1] [[:foo :baz] 2] ])
  ;;=> { :foo { :bar 1, :baz 2 } }
  "
  [data]
  (locking temporary-state
    (doseq [[k v] data] (swap! temporary-state assoc-in k v))))

(defn temp-empty
  "Erases the data saved in the temporary state."
  []
  (swap! temporary-state empty))

(defn ro-replace-with-temp
  "Replaces the global read-only state with the contents of the temporary state."
  []
  (TermPayloadClojureScoreQuery/replace_expr_global_state_ro @temporary-state))

(defn ro-merge-with-temp
  "Merges the temporary state with the global read-only one.

  Refer to bzzz.state.ro-merge for details"
  []
  (ro-merge @temporary-state))

(defn ro-deep-merge-with-temp
  "Deep-merges the temporary state with the global read-only one.

  Refer to bzzz.state.ro-deep-merge for details"
  []
  (ro-deep-merge @temporary-state))
