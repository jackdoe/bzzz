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
                       {:name_store_index_no_norms {:type "keyword" }})
          ret-1 (store test-index-name [{:id "WS baz bar"
                                         :name_store_index_no_norms "bar baz"
                                         :name_ngram_store_index_no_norms "andurilXX"
                                         :name_edge_ngram_store_index_no_norms "andurilXX"
                                         :name_keyword_store_index_no_norms "hello worldXX"
                                         :name_no_html_store_index_no_norms "bzbzXX<br><html>"
                                         :name_index "with space"}]
                       {:name_store_index_no_norms {:type "whitespace" }
                        :name_keyword_store_index_no_norms {:type "custom"
                                                            :tokenizer "keyword"
                                                            :char-filter [{:type "pattern-replace"
                                                                           :pattern "X+",
                                                                           :replacement "ZZ"}]}
                        :name_no_html_store_index_no_norms {:type "custom"
                                                            :tokenizer "whitespace"
                                                            :char-filter [{:type "pattern-replace"
                                                                           :pattern "X+",
                                                                           :replacement "ZZ"}
                                                                          {:type "html-strip",:escaped-tags ["br"]}]}
                        :name_edge_ngram_store_index_no_norms {:type "custom"
                                                               :tokenizer "edge-ngram"
                                                               :char-filter [{:type "pattern-replace"
                                                                              :pattern "X+",
                                                                              :replacement "ZZ"}]
                                                               :min_gram 1
                                                               :max_gram 8}
                        :name_ngram_store_index_no_norms {:type "custom"
                                                          :tokenizer "ngram"
                                                          :filter [{:type "lowercase"}]
                                                          :char-filter [{:type "pattern-replace"
                                                                        :pattern "X+",
                                                                        :replacement "ZZ"}]
                                                          :min_gram 2
                                                          :max_gram 4}})]
      (refresh-search-managers)
      (is (= true (ret-0 test-index-name)))
      (is (= true (ret-1 test-index-name)))))

  (testing "search-bool"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "id", :value "baz bar"}}]}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_index (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))

  (testing "search-no-html"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_no_html_store_index_no_norms", :value "bzbzZZ<br>"}}]
                                     :must-not [{:term {:field "name_no_html_store_index_no_norms", :value "bzbzZZ<br><html>"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "bzbzXX<br><html>" (:name_no_html_store_index_no_norms (first (:hits ret)))))))

  (testing "search-keyword"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_keyword_store_index_no_norms", :value "hello worldZZ"}}]
                                     :must-not [{:term {:field "name_keyword_store_index_no_norms", :value "hello worldXX"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "hello worldXX" (:name_keyword_store_index_no_norms (first (:hits ret)))))))

  (testing "search-ngram"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_ngram_store_index_no_norms", :value "an"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "and"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "andu"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "nd"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "ndu"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "ndur"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "du"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "dur"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "duri"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "ur"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "uri"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "uril"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "rilz"}}
                                            {:term {:field "name_ngram_store_index_no_norms", :value "ilzz"}}]
                                     :must-not [{:term {:field "name_ngram_store_index_no_norms", :value "anduril"}}
                                                {:term {:field "name_ngram_store_index_no_norms", :value "andurilxx"}}
                                                {:term {:field "name_ngram_store_index_no_norms", :value "andurilXX"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "andurilXX" (:name_ngram_store_index_no_norms (first (:hits ret)))))))


  (testing "search-edge-ngram"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_edge_ngram_store_index_no_norms", :value "an"}}
                                            {:term {:field "name_edge_ngram_store_index_no_norms", :value "andurilZ"}}]
                                     :must-not [{:term {:field "name_edge_ngram_store_index_no_norms", :value "andurilZZ"}}
                                                {:term {:field "name_edge_ngram_store_index_no_norms", :value "andurilXX"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "andurilXX" (:name_edge_ngram_store_index_no_norms (first (:hits ret)))))))

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
                      :analyzer {:name_store_index {:type "standard"} }
                      :highlight {:field "name_store_index"}
                      :query { :query-parser {:query "john@doe"
                                              :default-operator "or"
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
