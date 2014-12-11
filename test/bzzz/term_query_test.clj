(ns bzzz.term-query-test
  (:use clojure.test
        bzzz.core
        bzzz.util
        bzzz.index-store
        bzzz.index-search))
(def test-index-name "__lein-test-testing-index-term-test")

(deftest test-app
  (testing "cleanup-before"
    (delete-all test-index-name))

  (testing "query"
    (let [stored (store {:index test-index-name
                         :must-refresh true
                         :documents [{:name "jack doe"}
                                     {:name "john doe"}]})
          r0 (search {:index test-index-name
                      :query {:term {:field "name"
                                     :value "doe"}}})
          r1 (search {:index test-index-name
                      :query {:term {:field "name"
                                     :value "jack"}}})]
      (is (= 1 (:total r1)))
      (is (= 2 (:total r0)))))

  (testing "cleanup-after"
    (delete-all test-index-name)))
