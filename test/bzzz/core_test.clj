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

  (testing "search-or-pages"
    (let [ret-page-0 (search :index test-index-name
                      :default-field "name_store_index"
                      :default-operator "or"
                      :size 1
                      :page 0
                      :query "john doe")
          ret-page-1 (search :index test-index-name
                             :default-field "name_store_index"
                             :default-operator "or"
                             :size 1
                             :page 1
                             :query "john doe")
          ret-page-2 (search :index test-index-name
                             :default-field "name_store_index"
                             :default-operator "or"
                             :size 1
                             :page 2
                             :query "john doe")]
      (is (= 2 (:total ret-page-0)))
      (is (= 2 (:total ret-page-1)))
      (is (= 2 (:total ret-page-2)))
      (is (= 1 (count (:hits ret-page-0))))
      (is (= 1 (count (:hits ret-page-1))))
      (is (= 0 (count (:hits ret-page-2))))
      (is (= "john doe" (:name_store_index (first (:hits ret-page-0)))))
      (is (= "jack doe foo" (:name_store_index (last (:hits ret-page-1)))))))

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


