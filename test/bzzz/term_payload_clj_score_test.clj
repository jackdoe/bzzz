(ns bzzz.term-payload-clj-score-test
  (:use clojure.test
        bzzz.core
        bzzz.util
        bzzz.index-store
        bzzz.query
        bzzz.index-search))
(def test-index-name "__lein-test-testing-index-term-payload-clj-score-test")

(deftest test-app
  (testing "cleanup-before"
    (delete-all test-index-name))

  (testing "query"
    (let [x (reset! allow-unsafe-queries* true)
          stored (store {:index test-index-name
                         :must-refresh true
                         :documents [{:name_payload (str "zzzxxx|" 8)
                                      :some_integer 1
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30},
                                     {:name_payload (str "zzzxxx|" 8)
                                      :some_integer 2
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30},
                                     {:name_payload (str "zzzxxx|"7)
                                      :some_integer 2
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30}
                                     {:name_payload (str "zzzxxx|"7)
                                      :some_integer 2
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30}]
                         :facets {:name_payload {:use-analyzer "name_payload"}}
                         :analyzer {:name_payload {:type "custom"
                                                   :tokenizer "whitespace"
                                                   :filter [{:type "delimited-payload"}]}}})

          r-no-zero (search {:index test-index-name
                             :explain true
                             :facets {:name_payload {}}
                             :query {:no-zero-score
                                     {:query {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                                       :field-cache ["some_integer","some_float","some_double","some_long"]
                                                                       :fixed-bucket-aggregation [{:name "some_integer"
                                                                                                   :buckets 3},
                                                                                                  {:name "some_other"
                                                                                                   :buckets 31}]
                                                                       :clj-eval "
(fn [^bzzz.java.query.ExpressionContext ctx]
  (let [some_integer (.fc_get_int ctx \"some_integer\")
        some_float (.fc_get_float ctx \"some_float\")
        some_long (.fc_get_long ctx \"some_long\")
        some_double (.fc_get_double ctx \"some_double\")
        existed (.local_state_get ctx some_integer)
        payload (.payload_get_int ctx)]
    (.fba_aggregate_into_bucket ctx 0 some_integer 1)
    (.fba_aggregate_into_bucket ctx 1 some_long 1)
    (float
      1)))
"
                                                                       }}}}})
          r0 (search {:index test-index-name
                      :explain true
                      :facets {:name_payload {}}
                      :query {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                       :field-cache ["some_integer","some_float","some_double","some_long"]
                                                       :fixed-bucket-aggregation [{:name "some_integer"
                                                                                   :buckets 3},
                                                                                  {:name "some_other"
                                                                                   :buckets 31}]
                                                       :clj-eval "
(fn [^bzzz.java.query.ExpressionContext ctx]
  (let [some_integer (.fc_get_int ctx \"some_integer\")
        some_float (.fc_get_float ctx \"some_float\")
        some_long (.fc_get_long ctx \"some_long\")
        some_double (.fc_get_double ctx \"some_double\")
        existed (.local_state_get ctx some_integer)
        payload (.payload_get_int ctx)]
    (.fba_aggregate_into_bucket ctx 0 some_integer 1)
    (.fba_aggregate_into_bucket ctx 1 some_long 1)
    (float
      1)))
"
                                                       }}})
          r-no-facet (search {:index test-index-name
                              :explain true
                              :query {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                               :field-cache ["some_integer","some_float","some_double","some_long"]
                                                               :fixed-bucket-aggregation [{:name "some_integer"
                                                                                           :buckets 3},
                                                                                          {:name "some_other"
                                                                                           :buckets 31}]
                                                               :clj-eval "
(fn [^bzzz.java.query.ExpressionContext ctx]
  (let [some_integer (.fc_get_int ctx \"some_integer\")
        some_float (.fc_get_float ctx \"some_float\")
        some_long (.fc_get_long ctx \"some_long\")
        some_double (.fc_get_double ctx \"some_double\")
        existed (.local_state_get ctx some_integer)
        payload (.payload_get_int ctx)]
    (.fba_aggregate_into_bucket ctx 0 some_integer 1)
    (.fba_aggregate_into_bucket ctx 1 some_long 1)
    (float
      1)))
"
                                                               }}})]
      (is (= "zzzxxx" (:label (first (:name_payload (:facets r0))))))
      (is (= 4 (:count (first (:name_payload (:facets r0))))))
      (doseq [r [r0 r-no-facet r-no-zero]]
        (is (= 3 (:count (first (:some_integer (:facets r))))))
        (is (= 2 (:label (first (:some_integer (:facets r))))))
        (is (= 30 (:label (first (:some_other (:facets r))))))
        (is (= 4 (:count (first (:some_other (:facets r)))))))
      (reset! allow-unsafe-queries* false)))

  (testing "cleanup-after"
    (delete-all test-index-name)))
