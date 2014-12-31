(ns bzzz.term-payload-clj-score-test
  (:use clojure.test
        bzzz.core
        bzzz.util
        bzzz.analyzer
        bzzz.index-store
        bzzz.index-search
        bzzz.query)
  (:import (bzzz.java.analysis CodeTokenizer BytePayloadTokenizer)
           (bzzz.java.query Helper Helper$TermPayload)))

(def test-index-name "__lein-test-testing-index-term-payload-clj-score-test")

(deftest test-app
  (testing "cleanup-before"
    (delete-all test-index-name))

  ;; TODO(bnikolov) add tests for multi term queries
  (testing "query"
    (let [x (reset! allow-unsafe-queries* true)
          clj-eval "
(fn [^bzzz.java.query.ExpressionContext ctx]
  (let [some_integer (.fc_get_int ctx \"some_integer\")
        some_float (.fc_get_float ctx \"some_float\")
        some_long (.fc_get_long ctx \"some_long\")
        some_double (.fc_get_double ctx \"some_double\")
        existed (.local_state_get ctx some_integer)
        payload (.payload_get_int ctx)]
    (.payload_get_long ctx 5 6)
    (.fba_aggregate_into_bucket ctx 0 some_integer 1)
    (.fba_aggregate_into_bucket ctx 1 some_long 1)
    (float 1)))"
          stored (store {:index test-index-name
                         :must-refresh true
                         :documents [{:name_payload "zzzxxx|ffffffffffAABBCCDDEEFF zzzxxx|AAAAAAAA"
                                      :some_integer 1
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30},
                                     {:name_payload "zzzxxx|88"
                                      :some_integer 2
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30},
                                     {:name_payload "zzzxxx|88"
                                      :some_integer 2
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30}
                                     {:name_payload "zzzxxx|99"
                                      :some_integer 2
                                      :some_float 10.0
                                      :some_double 20.0
                                      :some_long 30}]
                         :facets {:name_payload {:use-analyzer "name_payload"}}
                         :analyzer {:name_payload {:type "custom"
                                                   :tokenizer "byte-payload"}}})

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
                                                                       :clj-eval clj-eval}}}}})
          r0 (search {:index test-index-name
                      :explain true
                      :facets {:name_payload {}}
                      :query {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                       :field-cache ["some_integer","some_float","some_double","some_long"]
                                                       :fixed-bucket-aggregation [{:name "some_integer"
                                                                                   :buckets 3},
                                                                                  {:name "some_other"
                                                                                   :buckets 31}]
                                                       :clj-eval clj-eval}}})
          r-nested (search {:index test-index-name
                            :explain false
                            :facets {:name_payload {}}
                            :query {:bool
                                    {:must
                                     [{:match-all {}}
                                      {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                                :field-cache ["some_integer","some_float","some_double","some_long"]
                                                                :fixed-bucket-aggregation [{:name "some_integer_top"
                                                                                            :buckets 3},
                                                                                           {:name "some_other_top"
                                                                                            :buckets 31}]
                                                                :clj-eval clj-eval}}
                                      {:dis-max
                                       {:queries
                                        [{:no-norm
                                          {:query
                                           {:no-zero-score
                                            {:query
                                             {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                                       :field-cache ["some_integer","some_float","some_double","some_long"]
                                                                       :fixed-bucket-aggregation [{:name "some_integer"
                                                                                                   :buckets 3},
                                                                                                  {:name "some_other"
                                                                                                   :buckets 31}]
                                                                       :clj-eval clj-eval}}}}}}
                                         {:no-norm
                                          {:query
                                           {:no-zero-score
                                            {:query
                                             {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                                       :field-cache ["some_integer","some_float","some_double","some_long"]
                                                                       :fixed-bucket-aggregation [{:name "some_integer"
                                                                                                   :buckets 3},
                                                                                                  {:name "some_other"
                                                                                                   :buckets 31}]
                                                                       :clj-eval clj-eval}}}}}}]}}]}}})

          r-no-facet (search {:index test-index-name
                              :explain true
                              :query {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                               :field-cache ["some_integer","some_float","some_double","some_long"]
                                                               :fixed-bucket-aggregation [{:name "some_integer"
                                                                                           :buckets 3},
                                                                                          {:name "some_other"
                                                                                           :buckets 31}]
                                                               :clj-eval clj-eval}}})]
      (is (= "zzzxxx" (:label (first (:name_payload (:facets r0))))))
      (is (= 4 (:count (first (:name_payload (:facets r0))))))

      (is (= 3 (:count (first (:some_integer_top (:facets r-nested))))))
      (is (= 2 (:label (first (:some_integer_top (:facets r-nested))))))
      (is (= 30 (:label (first (:some_other_top (:facets r-nested))))))
      (is (= 4 (:count (first (:some_other_top (:facets r-nested))))))
      (is (thrown-with-msg? Throwable
                            #"<fixed-bucket-aggregation>"
                            (search {:index test-index-name
                                     :explain true
                                     :query {:term-payload-clj-score {:field "name_payload", :value "zzzxxx"
                                                                      :field-cache ["some_integer","some_float","some_double","some_long"]
                                                                      :clj-eval clj-eval
                                                                      :fixed-bucket-aggregation-typo [{:name "some_integer"
                                                                                                       :buckets 3},
                                                                                                      {:name "some_other"
                                                                                                       :buckets 31}]}}})))
      (doseq [r [r0 r-no-facet r-no-zero]]
        (is (= 3 (:count (first (:some_integer (:facets r))))))
        (is (= 2 (:label (first (:some_integer (:facets r))))))
        (is (= 30 (:label (first (:some_other (:facets r))))))
        (is (= 4 (:count (first (:some_other (:facets r)))))))
      (reset! allow-unsafe-queries* false)))

  (testing "byte-payload-analyzer"
    (let [analyzer (parse-analyzer {"test" {:type "custom" :tokenizer "byte-payload"} })
          tokenized (Helper/tokenize_into_term_payload "test" "hello|aAbBcCdDeEff hello3|000102030405060708090a0b0c0d0e0f" analyzer)
          t0 ^Helper$TermPayload(.get tokenized 0)
          t1 ^Helper$TermPayload(.get tokenized 1)
          b0 (.bytes (.payload t0))
          b1 (.bytes (.payload t1))]

      (is (= 0xaa (bit-and (aget b0 0) 0xff)))
      (is (= 0xbb (bit-and (aget b0 1) 0xff)))
      (is (= 0xcc (bit-and (aget b0 2) 0xff)))
      (is (= 0xdd (bit-and (aget b0 3) 0xff)))
      (is (= 0xee (bit-and (aget b0 4) 0xff)))
      (is (= 0xff (bit-and (aget b0 5) 0xff)))

      (is (= 0x00 (bit-and (aget b1 0) 0xff)))
      (is (= 0x01 (bit-and (aget b1 1) 0xff)))
      (is (= 0x02 (bit-and (aget b1 2) 0xff)))
      (is (= 0x03 (bit-and (aget b1 3) 0xff)))
      (is (= 0x04 (bit-and (aget b1 4) 0xff)))
      (is (= 0x05 (bit-and (aget b1 5) 0xff)))
      (is (= 0x06 (bit-and (aget b1 6) 0xff)))
      (is (= 0x07 (bit-and (aget b1 7) 0xff)))
      (is (= 0x08 (bit-and (aget b1 8) 0xff)))
      (is (= 0x09 (bit-and (aget b1 9) 0xff)))
      (is (= 0x0a (bit-and (aget b1 10) 0xff)))
      (is (= 0x0b (bit-and (aget b1 11) 0xff)))
      (is (= 0x0c (bit-and (aget b1 12) 0xff)))
      (is (= 0x0d (bit-and (aget b1 13) 0xff)))
      (is (= 0x0e (bit-and (aget b1 14) 0xff)))
      (is (= 0x0f (bit-and (aget b1 15) 0xff)))

      (is (= "hello" (.term t0)))
      (is (= "hello3" (.term t1)))))

  (testing "nextPosition"
    (let [x (reset! allow-unsafe-queries* true)
          stored (store {:index test-index-name
                         :force-merge 1
                         :documents [
                                     {:advance "advance|000000cc000000cc000000cc", :name "bbb"}
                                     {:advance "advance|000000cc000000cc00000000", :name "aaa"}
                                     {:name "bbb"}
                                     ;; there was a bug in Helper.java, not calling nextPosition
                                     ;; when advance() succeeds, but does not return the requested target
                                     ;; this led to returning score 0xCC instead of 0xBB, while
                                     ;; on expain it properly returns 0xBB (due to the nature of explain
                                     ;; advances)
                                     ;; the bug was fixed in 4dbb021dc929bb95c0e401e5e5708ed89214398c
                                     {:advance "advance|000000cc000000cc000000bb", :name "bbb"}
                                     {:advance "advance|000000cc000000cc00000000", :name "bbb"}
                                     {:advance "advance|000000cc000000cc00000000", :name "aaa"}
                                     {:advance "advance|000000cc000000cc00000000", :name "aaa"}
                                     {:advance "advance|000000cc000000cc00000000", :name "aaa"}
                                     {:advance "advance|000000cc000000cc00000000", :name "aaa"}]
                         :analyzer {:advance {:type "custom" :tokenizer "byte-payload"}}})
          r-no-zero (search {:index test-index-name
                             :must-refresh true
                             :explain true
                             :size 1000
                             :query {:bool
                                     {:must [{:term-payload-clj-score {
                                                                       :field "advance", :value "advance"
                                                                       :no-zero true
                                                                       :clj-eval "
(fn [^bzzz.java.query.ExpressionContext ctx]
  (let [val (.payload_get_long ctx 8 4)]
    (.result-state-append ctx val)
    (float val)))"
                                                                       }}
                                             {:constant-score {:query
                                                               {:bool {:must [{:term {:field "name" :value "bbb"}}]}}
                                                               :boost 0}}]}}})
          x (reset! allow-unsafe-queries* false)]
      (is (> (count (:hits r-no-zero)) 0))
      (is (= 204.0 (:_score (first (:hits r-no-zero)))))
      (is (= 187.0 (:_score (last (:hits r-no-zero)))))
      (doseq [hit (:hits r-no-zero)]
        (is (= (int (:_score hit)) (int (.get ^java.util.List (first (:_result_state hit)) 0)))))))


  (testing "cleanup-after"
    (delete-all test-index-name)))
