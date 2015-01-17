(ns bzzz.state-test
  (:use clojure.test
        [bzzz.state :as state])
  (:import (bzzz.java.query TermPayloadClojureScoreQuery)))

(defn ro-get-in [k & [default]]
  (get-in TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO k default))

(defn ro-get-in-equals-to [k target & [msg]]
  (is (= (ro-get-in k) target)
      (str (if msg (str msg "\n") "") "Global state: " TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO)))

(defn temp-get-in-equals-to [k target & [msg] ]
  (is (= (get-in @state/temporary-state k) target)
      (str (if msg (str msg "\n") "") "Temporary state: " @state/temporary-state)))

(deftest test-state

  (testing "cleanup the global RO state leaves no key behind"
    (TermPayloadClojureScoreQuery/replace_expr_global_state_ro
      (hash-map))
    (is (= TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO {})))

  (testing "merge overwrites keys with their values"
    (state/ro-merge { :foo 1, :bar 2 })
    (is (= (ro-get-in [:foo]) 1))
    (is (= (ro-get-in [:bar]) 2)))

  (testing "deep-merge keeps nested keys untouched"
    (state/ro-deep-merge { :foo { :bar 1 }})
    (state/ro-deep-merge { :foo { :baz 2 }})
    (is (= (ro-get-in [:foo :bar]) 1) (str "Wrong nested key " TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO))
    (is (= (ro-get-in [:foo :baz]) 2) (str "Wrong nested key " TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO))

    (testing "while always the value associated if the key exists"
      (state/ro-deep-merge { :foo { :baz 3 }})
      (is (= (ro-get-in [:foo :baz]) 3) (str "Wrong nested key " TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO))))

  (testing "rename changes the key name"
    (let [test-data {:foo 1, :bar 2, :baz 3}]
      (state/ro-merge { :before-key test-data })
      (is (= (ro-get-in [:before-key]) test-data))
      (state/ro-rename-key :before-key :after-key)
      (is (= (ro-get-in [:after-key]) test-data))

      (testing "and makes sure the old key doesn't exist anymore"
        (is (nil? (ro-get-in [:before-key]))))

      (testing "and lets me be lousy rename using a string instead of a keyword. sigh."
        (state/ro-rename-key "after-key" "before-key")
        (ro-get-in-equals-to [:before-key] test-data))))

  (testing "temporary state"

    (testing "assoc-in adds keys properly"
      (state/temp-assoc-in [ [[:foo :bar] 42] [[:foo :baz] 24] ])
      (temp-get-in-equals-to [:foo :bar] 42)
      (temp-get-in-equals-to [:foo :baz] 24))

    (testing "replace overwrites the global state"
      ;; erase the global state
      (TermPayloadClojureScoreQuery/replace_expr_global_state_ro (hash-map))
      (state/ro-replace-with-temp)
      (ro-get-in-equals-to [:foo :bar] 42)
      (ro-get-in-equals-to [:foo :baz] 24))

    (testing "merge does the right thing"
      ;; preload the global state
      (TermPayloadClojureScoreQuery/replace_expr_global_state_ro { :foo { :bar 12 :foobar 1024 } })
      (state/ro-merge-with-temp)
      (ro-get-in-equals-to [:foo :foobar] nil "Subkeys should disapear")
      (ro-get-in-equals-to [:foo :bar] 42 "But collisions should be replaced"))

    (testing "deep-merge also does the right thing"
      ;; preload the global state
      (TermPayloadClojureScoreQuery/replace_expr_global_state_ro { :foo { :bar 12 :foobar 1024 } })
      (state/ro-deep-merge-with-temp)
      (ro-get-in-equals-to [:foo :foobar] 1024 "Old keys should not disappear")
      (ro-get-in-equals-to [:foo :bar] 42 "But collisions should be replaced"))

    (testing "and empty actually erases it"
      (state/temp-empty)
      (is (= @state/temporary-state {})))))
