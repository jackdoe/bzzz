(ns bzzz.state
  (use bzzz.util)
  (use [clojure.set :only (rename-keys)])
  (:import (java.util HashMap Map))
  (:import (bzzz.java.query TermPayloadClojureScoreQuery)))

;; FIXME Depending on the usage pattern, this can be terribly inefficient.
;;       If this becomes a problem, we should consider a flow where we
;;       write to the RW state and just merge into RO once the data pumping
;;       is complete.
;; FIXME All these conversions to java.util.HashMap seem really wasteful and
;;       make it all kinda silly, especially because it doesn't convert the
;;       values, so it probably beats the purpose. Get rid of it? Fix it?
;; TODO Maybe add support for @hosts?

(defn ro-merge
  "Merge the given data map into the global read-only state.

  This is useful for one-shot updates, where the whole state can
  safely fit into a request.

  Example:
  (ro-merge { :small-table [...] })

  The behavior is exactly the same as clojure.core/merge"
  [data]
  (TermPayloadClojureScoreQuery/replace_expr_global_state_ro
    (java.util.HashMap. ^java.util.Map
      (merge {} TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO data))))

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
    (java.util.HashMap. ^java.util.Map
      (deep-merge (into {} TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO) data))))

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
  (let [ro-state (into {} TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO)]
    (TermPayloadClojureScoreQuery/replace_expr_global_state_ro
      (java.util.HashMap. ^java.util.Map
        (rename-keys ro-state { (keyword from_key) (keyword to_key) })))))
