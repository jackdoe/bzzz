(ns bzzz.core-test
  (:use clojure.test
        bzzz.core
        bzzz.index))

(def test-index-name "lein-test-testing-index")
(deftest test-app
  (testing "cleanup"
    (delete-all test-index-name)
    (refresh-search-managers))

  (testing "store"
    (let [ret-0 (store test-index-name [{:name_store_index "jack doe foo"}
                                        {:name_store_index "john doe"}
                                        {:id "baz bar"
                                         :name_store_index_no_norms "bar baz"
                                         :name_index "with space"}]
                       {:name_store_index_no_norms { :use "keyword" }})
          ret-1 (store test-index-name [{:id "WS baz bar"
                                         :name_store_index_no_norms "bar baz"
                                         :name_index "with space"}]
                       {:name_store_index_no_norms { :use "whitespace" }})]
      (refresh-search-managers)
      (is (= true (ret-0 test-index-name)))
      (is (= true (ret-1 test-index-name)))))

  (testing "search-bool"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "id", :value "baz bar"}}]}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_index (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))

  (testing "search-changed-analyzer"
    (let [ret-kw (search :index test-index-name
                         :query {:term {:field "name_store_index_no_norms"
                                        :value "bar baz"}})
          ret-ws (search :index test-index-name
                         :query {:bool {:must [{:term {:field "name_store_index_no_norms"
                                                       :value "bar"}}
                                               {:term {:field "name_store_index_no_norms"
                                                       :value "baz"}}]}})]
      (is (= 1 (:total ret-kw)))
      (is (= 1 (:total ret-ws)))
      (is (= "WS baz bar" (:id (first (:hits ret-ws)))))
      (is (= "baz bar" (:id (first (:hits ret-kw)))))))

  (testing "search-bool-auto"
    (let [ret (search :index test-index-name
                      :query {:term {:field "id", :value "baz bar"}
                              :bool {:must ["name_index:space",
                                            {:term {:field "name_index", :value "with"}}]}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_index (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))


  (testing "search-or-standard-and-highlight"
    (let [ret (search :index test-index-name
                      :highlight {:field "name_store_index"}
                      :query { :query-parser {:query "john@doe"
                                              :default-operator "or"
                                              :analyzer {:name_store_index {:use "standard"} }
                                              :default-field "name_store_index"}})]
      (is (= 2 (:total ret)))
      (is (= "<b>john</b> <b>doe</b>" (:_highlight (first (:hits ret)))))
      (is (= "jack <b>doe</b> foo" (:_highlight (last (:hits ret)))))
      (is (= "john doe" (:name_store_index (first (:hits ret)))))
      (is (= "jack doe foo" (:name_store_index (last (:hits ret)))))))

  (testing "search-boost"
    (let [ret (search :index test-index-name
                      :explain false
                      :query { :bool {:should [
                                               {:bool {:must [
                                                              {:term {:field "name_store_index"
                                                                      :value "john"}}
                                                              {:term {:field "name_store_index"
                                                                      :value "doe"}}]}}
                                               {:term {:field "name_store_index"
                                                       :value "foo"
                                                       :boost 10}}]}})]
      (is (= 2 (:total ret)))
      (is (= "john doe" (:name_store_index (last (:hits ret)))))
      (is (= "jack doe foo" (:name_store_index (first (:hits ret)))))))

  (testing "search-or-pages"
    (let [ret-page-0 (search :index test-index-name
                             :query {:query-parser {:query "john doe"
                                                    :default-operator :or
                                                    :default-field "name_store_index"}}
                             :size 1
                             :page 0)
          ret-page-1 (search :index test-index-name
                             :query {:query-parser {:query "john doe"
                                                    :default-operator :or
                                                    :default-field "name_store_index"}}
                             :size 1
                             :page 1)
          ret-page-2 (search :index test-index-name
                             :query {:query-parser {:query "john doe"
                                                    :default-operator :or
                                                    :default-field "name_store_index"}}
                             :size 1
                             :page 2)]
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
                      :query {:query-parser {:query "john doe"
                                             :default-operator :and
                                             :default-field "name_store_index"}})]
      (is (= 1 (:total ret)))
      (is (= "john doe" (:name_store_index (first (:hits ret)))))))

  (testing "delete-by-query-and-search"
    (delete-from-query test-index-name "name_store_index:foo")
    (refresh-search-managers)
    (let [ret (search :index test-index-name
                      :query "name_store_index:doe")]
      (is (= 1 (:total ret)))
      (is (= "john doe" (:name_store_index (first (:hits ret)))))))

  (testing "cleanup-and-expect-zero"
    (delete-all test-index-name)
    (refresh-search-managers)
    (let [ret (search :index test-index-name
                      :default-field "name_store_index"
                      :query "doe")]
      (is (= 0 (:total ret)))))

  (testing "teardown"
    (shutdown)))
