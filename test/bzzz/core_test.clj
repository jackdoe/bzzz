(ns bzzz.core-test
  (:use clojure.test
        bzzz.core))

(def test-index-name "testing-index")
(deftest test-app
  (testing "cleanup"
    (delete-all test-index-name)
    (refresh-search-managers))

  (testing "store"
    (let [ret (store test-index-name [{:name_store_index "jack doe foo"}
                                      {:name_store_index "john doe"}])]
      (is (= true (ret test-index-name)))))

  (testing "search-or"
    (let [ret (search :index test-index-name
                      :default-field "name_store_index"
                      :default-operator "or"
                      :query "john doe")]
      (is (= 2 (:total ret)))
      (is (= "john doe" (:name_store_index (first (:hits ret)))))
      (is (= "jack doe foo" (:name_store_index (last (:hits ret)))))))

  (testing "search-and"
    (let [ret (search :index test-index-name
                      :default-field "name_store_index"
                      :default-operator "and"
                      :query "john doe")]
      (is (= 1 (:total ret)))
      (is (= "john doe" (:name_store_index (first (:hits ret)))))))

  (testing "delete-by-query-and-search"
    (delete-from-query test-index-name "name_store_index:foo")
    (refresh-search-managers)
    (let [ret (search :index test-index-name
                      :default-field "name_store_index"
                      :query "doe")]
      (is (= 1 (:total ret)))
      (is (= "john doe" (:name_store_index (first (:hits ret)))))))

  (testing "cleanup-and-expect-zero"
    (delete-all test-index-name)
    (refresh-search-managers)
    (let [ret (search :index test-index-name
                      :default-field "name_store_index"
                      :query "doe")]
      (is (= 0 (:total ret))))))


