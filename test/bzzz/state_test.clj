(ns bzzz.state-test
  (:use clojure.test
        [bzzz.state :as state])
  (:import (bzzz.java.query TermPayloadClojureScoreQuery)))

(defn ro-get-in [k & [default]]
  (get-in TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO k default))

(defn ro-get-in-equals-to [k target]
  (is (= (ro-get-in k) target)
      (str "Global State: " TermPayloadClojureScoreQuery/EXPR_GLOBAL_STATE_RO)))

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
        (ro-get-in-equals-to [:before-key] test-data)))))
