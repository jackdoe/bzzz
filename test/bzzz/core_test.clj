(ns bzzz.core-test
  (:import (java.io StringReader File))
  (:require [clojure.java.io :as io])
  (:require [clojure.data.json :as json])
  (:require [bzzz.query :as query])
  (:use clojure.test
        bzzz.core
        bzzz.util
        bzzz.const
        bzzz.index-directory
        bzzz.index-store
        bzzz.index-search))

(def test-index-name "__lein-test-testing-index")
(def moved-index-name "__lein-moved-test-testing-index")

(defn should-work [name expected]
  (let [ret (search {:index name
                     :facets {:find {}}
                     :query {:query-parser {:query "aaa bbb ccc"
                                            :default-operator :and
                                            :default-field "name_st_again"}}})]
    (is (= expected (:total ret)))
    (if-not (= 0 expected)
      (is (= "zzz" (:name (first (:hits ret))))))))

(defn leaves []
  (:leaves (:reader (get (index-stat) (sharded test-index-name 0)))))

(defn get-path ^File [p shard]
  (io/file (as-str default-root) (as-str default-identifier) (str (as-str p) "-shard-" (int-or-parse shard))))

(defn store-something [name shard]
  (store :index name
         :shard shard
         :documents [{:name "zzz" :name_st_again "aaa@bbb@ccc"
                      :find "zzz"
                      :priority_same_integer 1
                      :priority_integer 1
                      :priority_long 1
                      :priority_float 0.1
                      :priority_double 0.1
                      :lat_double 40.359011
                      :lon_double -73.9844722}
                     {:name "lll" :name_st_again "bbb@aaa"
                      :find "zzz zzz zzz"
                      :priority_same_integer 1
                      :priority_integer 2
                      :priority_long 2
                      :priority_float 0.2
                      :priority_double 0.2
                      :lat_double 40.759111
                      :lon_double -73.9844822}
                     {:priority_integer 3
                      :find "zzz"
                      :priority_same_integer 1
                      :priority_long 3
                      :priority_float 0.3
                      :priority_double 0.3
                      :lat_double 40.7143528
                      :lon_double -74.0059731}
                     {:priority_integer 4
                      :find "zzz"
                      :priority_same_integer 1
                      :priority_long 4
                      :priority_float 0.4
                      :priority_double 0.4
                      :lat_double 41.7143528
                      :lon_double -74.0059731}]
         :facets {:name {}
                  :name_st_again {:use-analyzer "bzbz-used-only-for-facet"}}
         :analyzer {:name_st_again {:type "standard"}
                    :bzbz-used-only-for-facet {:type "standard"}}))

(defn cleanup []
  (delete-all test-index-name)
  (refresh-search-managers)
  (let [ret (search {:index test-index-name
                     :default-field "name"
                     :query "doe"})]
    (is (= 0 (:total ret)))
    (refresh-search-managers)))

(defn rename [from to shard]
  (.renameTo (get-path from shard)
             (get-path to shard)))

(deftest test-app
  (testing "cleanup"
    (cleanup))

  (testing "store"
    (let [ret-0 (store :index test-index-name
                       :documents [{:name "jack doe foo"
                                    :age_integer "57"
                                    :long_long "570"
                                    :float_float "57.383"
                                    :double_double "570.383"}
                                   {:name ["john doe highlight","jack2 doe2 highlight",3,"highhlight"]
                                    :name_no_store ["new york","new york2",3]
                                    :age_integer ["67",64]
                                    :long_long ["670", 671]
                                    :float_float ["67.383", 67.384]
                                    :float_float_no_store ["67.383", 67.384]
                                    :double_double [670.383, 67.384]
                                    :dont_want "XXXXXXX"}
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
    (let [ret (search {:index test-index-name
                       :query {:bool {:must [{:term {:field "id", :value "baz bar"}}]}}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_no_store (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))

  (testing "search-dis-max"
    (let [ret (search {:index test-index-name
                       :explain true
                       :query {:dis-max {:queries [{:constant-score {:query {:term {:field "id", :value "baz bar"}}
                                                                     :boost 10}}
                                                   {:constant-score {:query {:term {:field "id", :value "baz bar"}}
                                                                     :boost 10}}]
                                         :tie-breaker "0.5"}}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_no_store (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))
      ;; FIXME create no norm similarity to test actual scores
      (is (not= (.indexOf (:_explain (first (:hits ret))) "max plus 0.5") -1))))

  (testing "search-wildcard"
    (let [r0 (search {:index test-index-name
                      :query {:wildcard {:field "id",
                                         :value "*baz*"}}})
          r1 (search {:index test-index-name
                      :query {:wildcard {:field "id",
                                         :value "*W? baz bar*"}}})]
      (is (= 2 (:total r0)))
      (is (= 1 (:total r1)))
      (is (= "WS baz bar" (:id (first (:hits r1)))))))

  (testing "search-fuzzy"
    (let [r0 (search {:index test-index-name
                      :query {:fuzzy {:field "id",
                                      :value "WS baz baz"}}})
          r1 (search {:index test-index-name
                      :query {:fuzzy {:field "id",
                                      :prefix-len 10
                                      :value "WS baz baz"}}})]
      (is (= 1 (:total r0)))
      (is (= 0 (:total r1)))))

  (testing "search-filter-query"
    (let [ret (search {:index test-index-name
                       :query {:filtered {:filter {:bool {:must [{:term {:field "id", :value "baz bar"}}]}}
                                          :query {:bool {:must [{:term {:field "name", :value "duplicate"}}]}}}}})
          ret-2 (search {:index test-index-name
                         :query {:bool {:must [{:term {:field "name", :value "duplicate"}}]}}})]
      (is (= 1 (:total ret)))
      (is (= 2 (:total ret-2)))
      (is (= (:_score (first (:hits ret))) (:_score (first (:hits ret-2)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))

  (testing "search-constant-score-query"
    (let [ret (search {:index test-index-name
                       :query {:constant-score {:boost 10
                                                :query {:bool {:must [{:term {:field "name", :value "duplicate"}}]}}}}})]
      (is (= 2 (:total ret)))
      (is (= (= (:_score (first (:hits ret))) (:_score (last (:hits ret))))) 10)))

  (testing "search-match-all"
    (let [ret (search {:index test-index-name
                       :query {:match-all {}}})
          num-docs (:docs (get (index-stat) (sharded test-index-name 0)))]
      (is (= (:total ret) num-docs))
      (is (> num-docs 0))))

  (testing "search-random-score-query"
    (let [ret (search {:index test-index-name
                       :explain true
                       :query {:bool {:must [{:match-all {}}
                                             {:random-score {:base 100
                                                             :query {:match-all {}}}}]}}})
          num-docs (:docs (get (index-stat) (sharded test-index-name 0)))]
      (is (= (:total ret) num-docs))
      (is (> (:_score (first (:hits ret))) 100))
      (is (not (= (:_score (first (:hits ret))) 100))) ;; well.. :D
      (is (> num-docs 0))))

  (testing "search-no-html"
    (let [ret (search {:index test-index-name
                       :query {:bool {:must [{:term {:field "name_no_html_no_norms", :value "bzbzZZ<br>"}}]
                                      :must-not [{:term {:field "name_no_html_no_norms", :value "bzbzZZ<br><html>"}}]}}})]
      (is (= 1 (:total ret)))
      (is (= "bzbzXX<br><html>" (:name_no_html_no_norms (first (:hits ret)))))))

  (testing "search-keyword"
    (let [ret (search {:index test-index-name
                       :query {:bool {:must [{:term {:field "name_keyword_no_norms", :value "hello worldZZ"}}]
                                      :must-not [{:term {:field "name_keyword_no_norms", :value "hello worldXX"}}]}}})]
      (is (= 1 (:total ret)))
      (is (= "hello worldXX" (:name_keyword_no_norms (first (:hits ret)))))))

  (testing "search-ngram"
    (let [ret (search {:index test-index-name
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
                                                 {:term {:field "name_ngram_no_norms", :value "andurilXX"}}]}}})]
      (is (= 1 (:total ret)))
      (is (= "andurilxX" (:name_ngram_no_norms (first (:hits ret)))))))


  (testing "search-edge-ngram"
    (let [ret (search {:index test-index-name
                       :query {:bool {:must [{:term {:field "name_edge_ngram_no_norms", :value "an"}}
                                             {:term {:field "name_edge_ngram_no_norms", :value "andurilZ"}}]
                                      :must-not [{:term {:field "name_edge_ngram_no_norms", :value "andurilZZ"}}
                                                 {:term {:field "name_edge_ngram_no_norms", :value "andurilXX"}}]}}})]
      (is (= 1 (:total ret)))
      (is (= "andurilXX" (:name_edge_ngram_no_norms (first (:hits ret)))))))

  (testing "search-changed-analyzer"
    (let [ret-kw (search {:index test-index-name
                          :query {:term {:field "name_no_norms"
                                         :value "bar baz"}}})
          ret-ws (search {:index test-index-name
                          :query {:bool {:must [{:term {:field "name_no_norms"
                                                        :value "bar"}}
                                                {:term {:field "name_no_norms"
                                                        :value "baz"}}]}}})]
      (is (= 1 (:total ret-kw)))
      (is (= 1 (:total ret-ws)))
      (is (= "WS baz bar" (:id (first (:hits ret-ws)))))
      (is (= "baz bar" (:id (first (:hits ret-kw)))))))

  (testing "search-bool-auto"
    (let [ret (search {:index test-index-name
                       :query {:term {:field "id", :value "baz bar"}
                               :bool {:must ["name_no_store:space",
                                             {:term {:field "name_no_store", :value "with"}}]}}})]
      (is (= 1 (:total ret)))
      (is (nil? (:name_no_store (first (:hits ret)))))
      (is (= "baz bar" (:id (first (:hits ret)))))))


  (testing "search-or-standard-and-highlight"
    (let [ret (search {:index test-index-name
                       :analyzer {:name {:type "standard"} }
                       :highlight {:fields ["name"]}
                       :explain true
                       :query {:query-parser {:query "john@doe"
                                              :default-operator "or"
                                              :default-field "name"}}})]
      (is (= 2 (:total ret)))
      (let [f (first (:hits ret))
            l (last (:hits ret))]
        (is (= "<b>john</b> <b>doe</b> highlight" (:text (first (:name (:_highlight f))))))
        (is (= "jack <b>doe</b> foo"  (:text (first (:name (:_highlight l))))))
        (is (= ["john doe highlight","jack2 doe2 highlight","3","highhlight"] (:name f)))
        (is (= "jack doe foo" (:name l))))))

  (testing "search-or-standard-and-highlight-fragments"
    (let [s "zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz XXX YYY zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz zzz"
          query {:query-parser {:query "xxx@yyy"
                                :default-operator "or"
                                :default-field "name"}}]
      (store :index test-index-name
             :documents [{:name s}]
             :analyzer {:name {:type "standard" }})

      (refresh-search-managers)
      (let [ret (search {:index test-index-name
                         :analyzer {:name {:type "standard"} }
                         :highlight {:fields ["name"]
                                     :pre ""
                                     :post ""}
                         :query query})]
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
                ret (search {:index test-index-name
                             :analyzer {:name {:type "standard"} }
                             :highlight {:fields ["name"]
                                         :pre "++"
                                         :post "++"}
                             :query query})
                d (first (:hits ret))
                f (first (:name (:_highlight d)))]
            (is (= (:name d) s))
            (is (= clean-pos-start (:text-start-pos f)))
            (is (= (+ 8 clean-pos-end) (:text-end-pos f))))))))



  (testing "search-or-standard-and-highlight-missing-field"
    (let [ret (search {:index test-index-name
                       :analyzer {:name {:type "standard"} }
                       :highlight {:fields ["name_should_be_missing"]}
                       :query {:query-parser {:query "john@doe"
                                              :default-operator "or"
                                              :default-field "name"}}})]
      (is (= (:name_should_be_missing (:_highlight (first (:hits ret)))) []))))

  (testing "search-boost"
    (let [ret (search {:index test-index-name
                       :explain false
                       :query {:bool {:should [{:bool {:must [{:term {:field "name"
                                                                      :value "john"}}
                                                              {:term {:field "name"
                                                                      :value "doe"}}]}}
                                               {:term {:field "name"
                                                       :value "foo"
                                                       :boost 10}}]}}})]
      (is (= 2 (:total ret)))
      (is (= ["john doe highlight","jack2 doe2 highlight","3","highhlight"] (:name (last (:hits ret)))))
      (is (= "jack doe foo" (:name (first (:hits ret)))))))

  (testing "search-or-pages"
    (let [ret-page-0 (search {:index test-index-name
                              :query {:query-parser {:query "john doe"
                                                     :default-operator :or
                                                     :default-field "name"}}
                              :size 1
                              :page 0})
          ret-page-1 (search {:index test-index-name
                              :query {:query-parser {:query "john doe"
                                                     :default-operator :or
                                                     :default-field "name"}}
                              :size 1
                              :page 1})
          ret-page-2 (search {:index test-index-name
                              :query {:query-parser {:query "john doe"
                                                     :default-operator :or
                                                     :default-field "name"}}
                              :size 1
                              :page 2})]
      (is (= 2 (:total ret-page-0)))
      (is (= 2 (:total ret-page-1)))
      (is (= 2 (:total ret-page-2)))
      (is (= 1 (count (:hits ret-page-0))))
      (is (= 1 (count (:hits ret-page-1))))
      (is (= 0 (count (:hits ret-page-2))))
      (is (= ["john doe highlight","jack2 doe2 highlight","3","highhlight"] (:name (first (:hits ret-page-0)))))
      (is (= "jack doe foo" (:name (last (:hits ret-page-1)))))))

  (testing "search-and"
    (let [ret (search {:index test-index-name
                       :query {:query-parser {:query "john doe"
                                              :default-operator :and
                                              :default-field "name"}}})]
      (is (= 1 (:total ret)))
      (is (= ["john doe highlight","jack2 doe2 highlight","3","highhlight"] (:name (first (:hits ret)))))))

  (testing "sorting"
    (store-something test-index-name 0)
    (refresh-search-managers)
    (let [s (fn [sort] (search {:index test-index-name
                                :query {:match-all {}}
                                :size 100
                                :sort sort}))
          is-nth-eq (fn [res key pos val]
                      (is (= (key (nth (:hits res) pos)) val)))
          r-test-syntax (s ["priority_integer"
                            {:expression "sqrt(_score) + priority_double + priority_float"
                             :reverse false
                             :explain true
                             :bindings ["priority_float"
                                        {:field :priority_double}]}
                            {:field :priority_long
                             :reverse true}
                            {:field "_score"
                             :reverse "false"}
                            "priority_double"
                            {:field "priority_double"
                             :reverse true}
                            "_doc"
                            "_score"
                            {:field "_doc"}])
          r-same (s :priority_same_integer)
          r-same-score (s [:priority_same_integer :_score])
          r-distance (s [:priority_same_integer
                         {:expression "haversin(40.7143528,-74.0059731,lat_double,lon_double)"
                          :order "asc"
                          :bindings [:lat_double
                                     :lon_double]}])
          r-pop-int (s [:priority_same_integer
                        :priority_integer])
          r-pop-int-rev (s [:priority_same_integer
                            {:field :priority_integer
                             :order "asc"}])

          r-pop-int-exp (s [:priority_same_integer
                            {:expression "sqrt(_score) + priority_integer"
                             :bindings [:priority_integer]}])

          r-pop-int-exp-rev (s [:priority_same_integer
                                {:expression "sqrt(_score) + priority_integer"
                                 :bindings [:priority_integer]
                                 :order "asc"}])

          r-doc-id-rev (s {:field :_doc :reverse true})
          r-doc-id (s {:field :_doc :reverse false})
          forward [[0 "1"] [1 "2"] [2 "3"] [3 "4"]]
          reverse [[0 "4"] [1 "3"] [2 "2"] [3 "1"]]]
      (let [fs (first (:_sort (first (:hits r-doc-id))))
            ls (first (:_sort (last (:hits r-doc-id-rev))))]
        (is (= (count (:hits r-doc-id)) (count (:hits r-doc-id-rev))))
        (is (> (count (:hits r-doc-id)) 0))
        (is (= (:value fs) (:value ls)))
        (is (= (:name fs) "_doc"))
        (is (= (:name fs) (:name ls)))
        (is (not= (:reverse fs) (:reverse ls))))
      (is (= (:priority_integer (nth (:hits r-distance) 0)) "3"))
      (is (= (:priority_integer (nth (:hits r-distance) 1)) "2"))
      (is (= (:priority_integer (nth (:hits r-distance) 2)) "1"))
      (is (= (:priority_integer (nth (:hits r-distance) 3)) "4"))

      (doseq [[pos val] forward]
        (is (= (:priority_integer (nth (:hits r-same) pos)) val)))

      (doseq [[pos val] forward]
        (is (= (:priority_integer (nth (:hits r-same-score) pos)) val)))

      (doseq [[pos val] reverse]
        (is (= (:priority_integer (nth (:hits r-pop-int) pos)) val)))

      (doseq [[pos val] reverse]
        (is (= (:priority_integer (nth (:hits r-pop-int-exp) pos)) val)))

      (doseq [[pos val] forward]
        (is (= (:priority_integer (nth (:hits r-pop-int-exp-rev) pos)) val)))

      (doseq [field [:priority_integer :priority_long :priority_double :priority_float]]
        (let [fo (s [:priority_same_integer
                     {:field field
                      :order "desc"}])
              fo-no-order (s [:priority_same_integer
                              field])
              exp (s [:priority_same_integer
                      {:expression (str "ln(_score) + " (as-str field))
                       :bindings [field]
                       :order "desc"}])
              exp-rev (s [:priority_same_integer
                          {:expression (str "ln(_score) + " (as-str field))
                           :bindings [field]
                           :order "asc"}])
              rev (s [:priority_same_integer
                      {:field field
                       :order "asc"}])]
          (doseq [[pos val] forward]
            (is (= (:priority_integer (nth (:hits rev) pos)) val)))
          (doseq [[pos val] forward]
            (is (= (:priority_integer (nth (:hits exp-rev) pos)) val)))
          (doseq [[pos val] reverse]
            (is (= (:priority_integer (nth (:hits exp) pos)) val)))
          (doseq [[pos val] reverse]
            (is (= (:priority_integer (nth (:hits fo-no-order) pos)) val)))
          (doseq [[pos val] reverse]
            (is (= (:priority_integer (nth (:hits fo) pos)) val)))))))

  (testing "custom-score"
    (let [r (search {:index test-index-name
                     :explain true
                     :query {:custom-score {:query {:range {:field "lat_double"}}
                                            :expression {:expression "-haversin(40.7143528,-74.0059731,lat_double,lon_double)"
                                                         :bindings ["lat_double"
                                                                    "lon_double"]}}}})
          h (:hits r)]
      (is (= -0.0 (:_score (nth h 0))))
      (is (= (float -2.6472278) (float (:_score (nth h 1)))))
      (is (= (float -19.771275) (float (:_score (nth h 2)))))
      (is (= (float -55.57916) (float (:_score (nth h 3)))))))

  (testing "expr-score"
    (let [r (search {:index test-index-name
                     :explain true
                     :query {:expr-score {:query {:term {:field "find", :value "zzz"}}
                                          :expression {:expression "(_score * 100) - haversin(40.7143528,-74.0059731,lat_double,lon_double)"
                                                       :bindings ["lat_double"
                                                                  "lon_double"]}}}})
          h (:hits r)]
      (is (= (float 158.77867) (:_score (nth h 0))))))


  (testing "search-range"
    (let [ret (search {:index test-index-name
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
                                                      :max-inclusive false}}]}}})
          ret-nil (search {:index test-index-name
                           :query {:range {:field "age_integer"}}})
          ret-ub (search {:index test-index-name
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
                                                         :max-inclusive false}}]}}})]
      (is (= 1 (:total ret)))
      (is (= 3 (:total ret-nil))) ;; all documents containing the age_integer field
      (let [r (first (:hits ret))]
        (is (= r (first (:hits ret-ub))))
        (is (= nil (:float_float_no_store r)))
        (is (= "47.383" (:float_float r)))
        (is (= "470.383" (:double_double r)))
        (is (= "47" (:age_integer r)))
        (is (= "470" (:long_long r))))))

  (testing "search-and"
    (let [ret (search {:index test-index-name
                       :highlight {:fields ["name","age_integer"]}
                       :fields {:age_integer true
                                :name true
                                :long_long true
                                :double_double true
                                :float_float true}
                       :query {:bool {:must [{:range {:field "age_integer" :min 63 :max 65}}
                                             {:term {:field "name"
                                                     :value "highlight"}}
                                             {:term {:field "name"
                                                     :value "3"}}
                                             {:term {:field "name"
                                                     :value "jack2"}}]}}})]
      (is (= 1 (:total ret)))
      (let [h (first (:hits ret))
            hi (:name (:_highlight h))]
        (is (= '() (:age_integer (:_highlight h))))
        (is (= nil (:float_float_no_store h)))
        (is (= nil (:dont_want h)))
        (is (= ["670" "671"] (:long_long h)))
        (is (= ["67" "64"] (:age_integer h)))
        (is (= ["670.383" "67.384"] (:double_double h)))
        (is (= ["67.383" "67.384"] (:float_float h)))
        (is (= 0 (:index (first hi))))
        (is (= 2 (:index (last hi))))
        (is (= "<b>3</b>" (:text (last hi))))
        (is (= ["john doe highlight","jack2 doe2 highlight","3","highhlight"] (:name h))))))


  (testing "delete-by-query-and-search"
    (delete-from-query test-index-name "name:foo")
    (refresh-search-managers)
    (let [ret (search {:index test-index-name
                       :query "name:doe"})]
      (is (= 1 (:total ret)))
      (is (= ["john doe highlight","jack2 doe2 highlight","3","highhlight"] (:name (first (:hits ret)))))))

  (testing "cleanup-and-expect-zero"
    (cleanup))

  (testing "write-exception-rolled-back"
    (dotimes [n 100]
      (is (thrown? Throwable
                   (store :index test-index-name
                          :documents [{:id "_aaa_" :name "zzz" :name_st "aaa@bbb"}
                                      {:name "lll" :name_st "bbb@aaa"}
                                      {:name_no_store_no_index "exception"}]
                          :facets {:name {}
                                   :name_st {:use-analyzer "bzbz-used-only-for-facet"}}
                          :analyzer {:name {:type "keyword"}
                                     :bzbz-used-only-for-facet {:type "standard"}})))
      (refresh-search-managers)
      (let [ret (search {:index test-index-name
                         :facets {:name {}, :name_st {}}
                         :query {:term {:field "name", :value "zzz"}}})]
        (is (= 0 (:total ret))))))


  (testing "leaves"
    (let [storer (fn [force-merge]
                   (store :index test-index-name
                          :documents [{:id "_aaabbb_" :name_leaves "zzz"}
                                      {:name_leaves "lll"}]
                          :force-merge force-merge
                          :facets {:name_leaves {:use-analyzer "bzbz-used-only-for-facet"}}
                          :analyzer {:bzbz-used-only-for-facet {:type "standard"}})
                   (refresh-search-managers))]

      (storer 0)
      (is (= 1 (leaves)))
      (storer 0)
      (is (= 2 (leaves)))
      (storer 0)
      (is (= 3 (leaves)))
      (storer 1)
      (is (= 1 (leaves)))
      (storer 0)
      (is (= 2 (leaves)))
      (storer 1)
      (is (= 1 (leaves)))))

  (testing "geo"
    (let [storer (fn [shape int]
                   (store :index test-index-name
                          :documents [{:id (str "_aa_bb_" shape) :sort_int int :name_geo "zzz" :__location shape}]
                          :facets {:name_geo {:use-analyzer "bzbz-used-only-for-facet"}}
                          :analyzer {:bzbz-used-only-for-facet {:type "standard"}})
                   (refresh-search-managers))
          searcher (fn [spatial reverse]
                     (search {:index test-index-name
                              :explain true
                              :sort [{:field "__location",
                                      :reverse reverse
                                      :point "POINT(10 -10)"},
                                     {:field "sort_int"
                                      :order "asc"}
                                     "_score"]
                              :facets {:name_geo {}}
                              :spatial-filter spatial
                              :query {:term {:field "name_geo", :value "zzz"}}}))]
      (storer "POINT(60.9289094 -50.7693246)" 0)
      (storer "POINT(10.9289094 -10.7693246)" 2)
      (storer "POINT(10.9289094 -10.7693246) " 1)
      (storer "POINT(11.9289094 -11.7693246)" 10)
      (is (= 4 (:total (searcher nil false))))
      (is (= 1.1944379994247891 (:value (first (:_sort (first (:hits (searcher nil false))))))))

      (is (= 4 (:count (first (:name_geo (:facets (searcher nil false)))))))
      (let [r (searcher "Intersects(BUFFER(POINT(10 -10),10))" false)
            r1 (searcher "Intersects(BUFFER(POINT(10 -10),10))" true)]
        (is (= 3 (:total r)))
        (is (= 3 (:total r1)))
        (is (= 1.1944379994247891 (:value (first (:_sort (nth (:hits r) 0))))))
        (is (= 1.1944379994247891 (:value (first (:_sort (nth (:hits r) 1))))))
        (is (= 2.591949181622539 (:value (first (:_sort (nth (:hits r) 2))))))
        (is (= 2.591949181622539 (:value (first (:_sort (nth (:hits r1) 0))))))
        (is (= 1.1944379994247891 (:value (first (:_sort (nth (:hits r1) 1))))))
        (is (= 1.1944379994247891 (:value (first (:_sort (nth (:hits r1) 2))))))

        (let [fs (:_sort (first (:hits r)))
              ls (:_sort (first (:hits r1)))]
          (is (= 1 (:value (second fs))))
          (is (= 10 (:value (second ls))))))
      (is (= 0 (:total (searcher "Intersects(BUFFER(POINT(10 -10),0))" false))))
      (is (= 1 (:total (searcher "Intersects(BUFFER(POINT(60 -49),10))" false))))))

  (testing "payload"
    (let [storer (fn [payload]
                   (store :index test-index-name
                          :documents [{:id (str "_aa_bb_" payload) :name_payload (str "zzz|" payload)}]
                          :facets {:name_payload {:use-analyzer "name_payload"}}
                          :analyzer {:name_payload {:type "custom"
                                                    :tokenizer "whitespace"
                                                    :filter [{:type "delimited-payload"
                                                              :delimiter "|"}]}})
                   (refresh-search-managers))
          searcher (fn []
                     (search {:index test-index-name
                              :explain true
                              :facets {:name_payload {}}
                              :query {:term-payload-clj-score {:field "name_payload", :value "zzz"
                                                               :clj-eval "(fn [payload] (+ 10 payload))"}}}))]
      (storer "255")
      (storer "1024")
      (is (thrown? Throwable ;; confirm that throws exception when unsafe queries are disabled
                   (searcher)))
      (reset! query/allow-unsafe-queries* true)
      (let [r (searcher)]
        (is (= 1034.0 (:_score (first (:hits r)))))
        (is (= 265.0 (:_score (second (:hits r))))))))

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
      (let [ret (search {:index test-index-name
                         :facets {:name {:size 1}, :name_st {:path ["uuu"]}}
                         :query {:match-all {}}})
            f (:facets ret)
            nf (:name f)
            ns (:name_st f)]
        (is (= (count nf) 1))
        (is (= (count ns) 4))
        (is (= (+ 1 n) (:count (first nf)))))))

  (testing "rename-the-index-refresh-manager"
    (store-something test-index-name 0)
    (refresh-search-managers)
    (should-work test-index-name 1)

    (rename test-index-name moved-index-name 0)
    (should-work moved-index-name 1)

    (store-something moved-index-name 0)
    (refresh-search-managers)

    (should-work moved-index-name 2)

    (rename moved-index-name test-index-name 0)

    (should-work test-index-name 2)

    (store-something test-index-name 0)
    (refresh-search-managers)
    (should-work test-index-name 3))

  (testing "sharding"
    (cleanup)
    (store-something test-index-name 0)
    (refresh-search-managers)
    (should-work test-index-name 1)
    (store-something test-index-name 1)
    (refresh-search-managers)
    (should-work test-index-name 2)
    (refresh-search-managers)
    (store-something test-index-name 3)
    (refresh-search-managers)
    (should-work test-index-name 3)
    (cleanup)
    (should-work test-index-name 0))

  (testing "redis"
    (cleanup)
    (store-something test-index-name 0)
    (refresh-search-managers)
    (should-work test-index-name 1)
    (try-create-prefix (get-path test-index-name 10))
    (spit (io/file (get-path test-index-name 10) "redis.conf")
          (json/write-str {:host "localhost"
                           :port 6379}))
    (refresh-search-managers)
    (store-something test-index-name 10)
    (refresh-search-managers)
    (is (= 2 (count (file-seq (get-path test-index-name 10)))))
    (should-work test-index-name 2)
    (cleanup)
    (should-work test-index-name 0))

  (testing "teardown"
    (shutdown)
    (try
      (do
        (delete-recursively (get-path test-index-name 0))
        (delete-recursively (get-path test-index-name 10))
        (delete-recursively (get-path test-index-name 1))
        (delete-recursively (get-path test-index-name 2))
        (delete-recursively (get-path test-index-name 3))
        (delete-recursively (get-path moved-index-name 0)))
      (catch Exception e))))
