(ns bzzz.core-test
  (:import (java.io File))
  (:use clojure.test
        bzzz.core
        bzzz.index))

(def test-index-name "lein-test-testing-index")

(defn should-work []
  (let [ret (search :index test-index-name
                    :query {:query-parser {:query "john doe"
                                           :default-operator :and
                                           :default-field "name"}})]
    (is (= 1 (:total ret)))
    (is (= "john doe" (:name (first (:hits ret)))))))

(deftest test-app
  (testing "cleanup"
    (delete-all test-index-name)
    (refresh-search-managers))

  (testing "store"
    (let [ret-0 (store :index test-index-name
                       :documents [{:name "jack doe foo"
                                    :age_integer "57"
                                    :long_long "570"
                                    :float_float "57.383"
                                    :double_double "570.383"}
                                   {:name "john doe"
                                    :age_integer "67"
                                    :long_long "670"
                                    :float_float "67.383"
                                    :double_double "670.383"}
                                   {:id "baz bar"
                                    :name "duplicate",
                                    :name_no_norms "bar baz"
                                    :age_integer "47"
                                    :long_long "470"
                                    :float_float "47.383"
                                    :float_float_no_store "47.383"
                                    :double_double "470.383"
                                    :filterable_no_store_integer "470"
                                    :name_no_store "with space"}]
                       :analyzer {:name_no_norms {:type "keyword" }})
          ret-1 (store :index test-index-name
                       :documents [{:id "WS baz bar"
                                    :name "duplicate"
                                    :name_no_norms "bar baz"
                                    :name_ngram_no_norms "andurilxX"
                                    :name_edge_ngram_no_norms "andurilXX"
                                    :name_keyword_no_norms "hello worldXX"
                                    :name_no_html_no_norms "bzbzXX<br><html>"
                                    :name_no_store "with space"}]
                       :analyzer {:name_no_norms {:type "whitespace" }
                                  :name_keyword_no_norms {:type "custom"
                                                          :tokenizer "keyword"
                                                          :char-filter [{:type "pattern-replace"
                                                                         :pattern "X+",
                                                                         :replacement "ZZ"}]}
                                  :name_no_html_no_norms {:type "custom"
                                                          :tokenizer "whitespace"
                                                          :char-filter [{:type "pattern-replace"
                                                                         :pattern "X+",
                                                                         :replacement "ZZ"}
                                                                        {:type "html-strip",:escaped-tags ["br"]}]}
                                  :name_edge_ngram_no_norms {:type "custom"
                                                             :tokenizer "edge-ngram"
                                                             :char-filter [{:type "pattern-replace"
                                                                            :pattern "X+",
                                                                            :replacement "ZZ"}]
                                                             :min_gram 1
                                                             :max_gram 8}
                                  :name_ngram_no_norms {:type "custom"
                                                        :tokenizer "ngram"
                                                        :filter [{:type "lowercase"}]
                                                        :char-filter [{:type "pattern-replace"
                                                                       :pattern "(?i)X+",
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
      (is (nil? (:name_no_store (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))

  (testing "search-filter-query"
    (let [ret (search :index test-index-name
                      :query {:filtered {:filter {:bool {:must [{:term {:field "id", :value "baz bar"}}]}}
                                         :query {:bool {:must [{:term {:field "name", :value "duplicate"}}]}}}})
          ret-2 (search :index test-index-name
                        :query {:bool {:must [{:term {:field "name", :value "duplicate"}}]}})]
      (is (= 1 (:total ret)))
      (is (= 2 (:total ret-2)))
      (is (= (:_score (first (:hits ret))) (:_score (first (:hits ret-2)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))

  (testing "search-constant-score-query"
    (let [ret (search :index test-index-name
                      :query {:constant-score {:boost 10
                                               :query {:bool {:must [{:term {:field "name", :value "duplicate"}}]}}}})]
      (is (= 2 (:total ret)))
      (is (= (= (:_score (first (:hits ret))) (:_score (last (:hits ret))))) 10)))

  (testing "search-match-all"
    (let [ret (search :index test-index-name
                      :query {:match-all {}})
          num-docs (:docs (get (index-stat) test-index-name))]
      (is (= (:total ret) num-docs))
      (is (> num-docs 0))))


  (testing "search-random-score-query"
    (let [ret (search :index test-index-name
                      :explain true
                      :query {:bool {:must [{:match-all {}}
                                            {:random-score {:base 100
                                                            :query {:match-all {}}}}]}})
          num-docs (:docs (get (index-stat) test-index-name))]
      (is (= (:total ret) num-docs))
      (is (> (:_score (first (:hits ret))) 100))
      (is (not (= (:_score (first (:hits ret))) 100))) ;; well.. :D
      (is (> num-docs 0))))

  (testing "search-no-html"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_no_html_no_norms", :value "bzbzZZ<br>"}}]
                                     :must-not [{:term {:field "name_no_html_no_norms", :value "bzbzZZ<br><html>"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "bzbzXX<br><html>" (:name_no_html_no_norms (first (:hits ret)))))))

  (testing "search-keyword"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_keyword_no_norms", :value "hello worldZZ"}}]
                                     :must-not [{:term {:field "name_keyword_no_norms", :value "hello worldXX"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "hello worldXX" (:name_keyword_no_norms (first (:hits ret)))))))

  (testing "search-ngram"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_ngram_no_norms", :value "an"}}
                                            {:term {:field "name_ngram_no_norms", :value "and"}}
                                            {:term {:field "name_ngram_no_norms", :value "andu"}}
                                            {:term {:field "name_ngram_no_norms", :value "nd"}}
                                            {:term {:field "name_ngram_no_norms", :value "ndu"}}
                                            {:term {:field "name_ngram_no_norms", :value "ndur"}}
                                            {:term {:field "name_ngram_no_norms", :value "du"}}
                                            {:term {:field "name_ngram_no_norms", :value "dur"}}
                                            {:term {:field "name_ngram_no_norms", :value "duri"}}
                                            {:term {:field "name_ngram_no_norms", :value "ur"}}
                                            {:term {:field "name_ngram_no_norms", :value "uri"}}
                                            {:term {:field "name_ngram_no_norms", :value "uril"}}
                                            {:term {:field "name_ngram_no_norms", :value "rilz"}}
                                            {:term {:field "name_ngram_no_norms", :value "ilzz"}}]
                                     :must-not [{:term {:field "name_ngram_no_norms", :value "anduril"}}
                                                {:term {:field "name_ngram_no_norms", :value "andurilxX"}}
                                                {:term {:field "name_ngram_no_norms", :value "andurilXX"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "andurilxX" (:name_ngram_no_norms (first (:hits ret)))))))


  (testing "search-edge-ngram"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:term {:field "name_edge_ngram_no_norms", :value "an"}}
                                            {:term {:field "name_edge_ngram_no_norms", :value "andurilZ"}}]
                                     :must-not [{:term {:field "name_edge_ngram_no_norms", :value "andurilZZ"}}
                                                {:term {:field "name_edge_ngram_no_norms", :value "andurilXX"}}]}})]
      (is (= 1 (:total ret)))
      (is (= "andurilXX" (:name_edge_ngram_no_norms (first (:hits ret)))))))

  (testing "search-changed-analyzer"
    (let [ret-kw (search :index test-index-name
                         :query {:term {:field "name_no_norms"
                                        :value "bar baz"}})
          ret-ws (search :index test-index-name
                         :query {:bool {:must [{:term {:field "name_no_norms"
                                                       :value "bar"}}
                                               {:term {:field "name_no_norms"
                                                       :value "baz"}}]}})]
      (is (= 1 (:total ret-kw)))
      (is (= 1 (:total ret-ws)))
      (is (= "WS baz bar" (:id (first (:hits ret-ws)))))
      (is (= "baz bar" (:id (first (:hits ret-kw)))))))

  (testing "search-bool-auto"
    (let [ret (search :index test-index-name
                      :query {:term {:field "id", :value "baz bar"}
                              :bool {:must ["name_no_store:space",
                                            {:term {:field "name_no_store", :value "with"}}]}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_no_store (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))


  (testing "search-or-standard-and-highlight"
    (let [ret (search :index test-index-name
                      :analyzer {:name {:type "standard"} }
                      :highlight {:fields ["name"]}
                      :query { :query-parser {:query "john@doe"
                                              :default-operator "or"
                                              :default-field "name"}})]
      (is (= 2 (:total ret)))
      (let [f (first (:hits ret))
            l (last (:hits ret))]
        (is (= "<b>john</b> <b>doe</b>" (:text (first (:name (:_highlight f))))))
        (is (= "jack <b>doe</b> foo"  (:text (first (:name (:_highlight l))))))
        (is (= "john doe" (:name f)))
        (is (= "jack doe foo" (:name l))))))

  (testing "search-or-standard-and-highlight-fragments"
    (let [s "zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz XXX YYY zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz"
          query { :query-parser {:query "xxx@yyy"
                                 :default-operator "or"
                                 :default-field "name"}}]
      (store :index test-index-name
             :documents [{:name s}]
             :analyzer {:name {:type "standard" }})

      (refresh-search-managers)
      (let [ret (search :index test-index-name
                        :analyzer {:name {:type "standard"} }
                        :highlight {:fields ["name"]
                                    :pre ""
                                    :post ""}
                        :query query)]
        (is (= 1 (:total ret)))
        (let [first-d (first (:hits ret))
              first-f (first (:name (:_highlight first-d)))]

          (is (= (:name first-d) s))
          (is (= (:text first-f) (subs (:name first-d)
                                       (:text-start-pos first-f)
                                       (:text-end-pos first-f))))
          (is (= 1199 (:text-start-pos first-f)))
          (is (= 1375 (:text-end-pos first-f)))
          (let [clean-pos-start (:text-start-pos first-f)
                clean-pos-end (:text-end-pos first-f)
                ret (search :index test-index-name
                            :analyzer {:name {:type "standard"} }
                            :highlight {:fields ["name"]
                                        :pre "++"
                                        :post "++"}
                            :query query)
                d (first (:hits ret))
                f (first (:name (:_highlight d)))]
            (is (= (:name d) s))
            (is (= clean-pos-start (:text-start-pos f)))
            (is (= (+ 8 clean-pos-end) (:text-end-pos f))))))))



  (testing "search-or-standard-and-highlight-missing-field"
    (let [ret (search :index test-index-name
                      :analyzer {:name {:type "standard"} }
                      :highlight {:fields ["name_should_be_missing"]}
                      :query { :query-parser {:query "john@doe"
                                              :default-operator "or"
                                              :default-field "name"}})]
      (is (= (:name_should_be_missing (:_highlight (first (:hits ret)))) []))))

  (testing "search-boost"
    (let [ret (search :index test-index-name
                      :explain false
                      :query { :bool {:should [
                                               {:bool {:must [
                                                              {:term {:field "name"
                                                                      :value "john"}}
                                                              {:term {:field "name"
                                                                      :value "doe"}}]}}
                                               {:term {:field "name"
                                                       :value "foo"
                                                       :boost 10}}]}})]
      (is (= 2 (:total ret)))
      (is (= "john doe" (:name (last (:hits ret)))))
      (is (= "jack doe foo" (:name (first (:hits ret)))))))

  (testing "search-or-pages"
    (let [ret-page-0 (search :index test-index-name
                             :query {:query-parser {:query "john doe"
                                                    :default-operator :or
                                                    :default-field "name"}}
                             :size 1
                             :page 0)
          ret-page-1 (search :index test-index-name
                             :query {:query-parser {:query "john doe"
                                                    :default-operator :or
                                                    :default-field "name"}}
                             :size 1
                             :page 1)
          ret-page-2 (search :index test-index-name
                             :query {:query-parser {:query "john doe"
                                                    :default-operator :or
                                                    :default-field "name"}}
                             :size 1
                             :page 2)]
      (is (= 2 (:total ret-page-0)))
      (is (= 2 (:total ret-page-1)))
      (is (= 2 (:total ret-page-2)))
      (is (= 1 (count (:hits ret-page-0))))
      (is (= 1 (count (:hits ret-page-1))))
      (is (= 0 (count (:hits ret-page-2))))
      (is (= "john doe" (:name (first (:hits ret-page-0)))))
      (is (= "jack doe foo" (:name (last (:hits ret-page-1)))))))

  (testing "search-and"
    (let [ret (search :index test-index-name
                      :query {:query-parser {:query "john doe"
                                             :default-operator :and
                                             :default-field "name"}})]
      (is (= 1 (:total ret)))
      (is (= "john doe" (:name (first (:hits ret)))))))

  (testing "search-and"
    (let [ret (search :index test-index-name
                      :query {:bool {:must [{:range {:field "age_integer"
                                                     :min "45"
                                                     :max 47
                                                     :min-inclusive true
                                                     :max-inclusive true}}
                                            {:range {:field "long_long"
                                                     :min "470"
                                                     :max 471
                                                     :min-inclusive true
                                                     :max-inclusive false}}
                                            {:range {:field "float_float"
                                                     :min "47.01"
                                                     :max "47.99"
                                                     :min-inclusive false
                                                     :max-inclusive false}}
                                            {:range {:field "double_double"
                                                     :min "470.01"
                                                     :max "470.99"
                                                     :min-inclusive false
                                                     :max-inclusive false}}]}})
          ret-nil (search :index test-index-name
                          :query {:range {:field "age_integer"}})
          ret-ub (search :index test-index-name
                         :query {:bool {:must [{:range {:field "age_integer"
                                                        :min "45"
                                                        :max nil
                                                        :min-inclusive true
                                                        :max-inclusive true}}
                                               {:range {:field "long_long"
                                                        :min nil
                                                        :max nil
                                                        :min-inclusive true
                                                        :max-inclusive false}}
                                               {:range {:field "float_float"
                                                        :min nil
                                                        :max "47.99"
                                                        :min-inclusive false
                                                        :max-inclusive false}}
                                               {:range {:field "double_double"
                                                        :min "470.01"
                                                        :max nil
                                                        :min-inclusive false
                                                        :max-inclusive false}}]}})]
      (is (= 1 (:total ret)))
      (is (= 3 (:total ret-nil))) ;; all documents containing the age_integer field
      (let [r (first (:hits ret))]
        (is (= r (first (:hits ret-ub))))
        (is (= nil (:float_float_no_store r)))
        (is (= "47.383" (:float_float r)))
        (is (= "470.383" (:double_double r)))
        (is (= "47" (:age_integer r)))
        (is (= "470" (:long_long r))))))

  (testing "delete-by-query-and-search"
    (delete-from-query test-index-name "name:foo")
    (refresh-search-managers)
    (let [ret (search :index test-index-name
                      :query "name:doe")]
      (is (= 1 (:total ret)))
      (is (= "john doe" (:name (first (:hits ret)))))))

  (testing "cleanup-and-expect-zero"
    (delete-all test-index-name)
    (refresh-search-managers)
    (let [ret (search :index test-index-name
                      :default-field "name"
                      :query "doe")]
      (is (= 0 (:total ret)))))

  (testing "write-exception-rolled-back"
    (dotimes [n 100]
      (is (thrown? Throwable
                   (store :index test-index-name
                          :documents [{:name "zzz" :name_st "aaa@bbb"}
                                      {:name "lll" :name_st "bbb@aaa"}
                                      {:name_no_store_no_index "exception"}]
                          :facets {:name {}
                                   :name_st {:use-analyzer "bzbz-used-only-for-facet"}}
                          :analyzer {:name {:type "keyword"}
                                     :bzbz-used-only-for-facet {:type "standard"}})))
      (refresh-search-managers)
      (let [ret (search :index test-index-name
                        :facets {:name {}, :name_st {}}
                        :query {:term {:field "name", :value "zzz"}})]
        (is (= 0 (:total ret))))))

  (testing "facets"
    (dotimes [n 1000]
      (store :index test-index-name
             :documents [{:name "abc" :name_st "ddd mmm"}
                         {:name "def 123" :name_st "uuu ooo"}]
             :facets {:name {}
                      :name_st {:use-analyzer "bzbz-used-only-for-facet"}}
             :analyzer {:name {:type "keyword"}
                        :bzbz-used-only-for-facet {:type "standard"}})
      (refresh-search-managers)
      (let [ret (search :index test-index-name
                        :facets {:name {:size 1}, :name_st {:path ["uuu"]}}
                        :query {:match-all {}})
            f (:facets ret)
            nf (:name f)
            ns (:name_st f)]
        (is (= (count nf) 1))
        (is (= (count ns) 4))
        (is (= (+ 1 n) (:count (first nf)))))))

  (testing "teardown"
    (shutdown)))
