(ns bzzz.sharding-test
  (:use clojure.test
        bzzz.core
        bzzz.util
        bzzz.analyzer
        bzzz.index-store
        bzzz.index-search
        bzzz.query))

(def test-index-name "__lein-test-testing-index-sharding-test")

(deftest test-app
  (testing "cleanup-before"
    (delete-all test-index-name))

  (testing "sharding"
    (dotimes [x 100]
      (let [doc-generator (fn [n]
                            {:name "jack"
                             :id (str n)})
            list-generator (fn [n]
                             (into []
                                   (for [i (range n)]
                                     (doc-generator i))))
            docs (list-generator 1000)]
        (delete-all test-index-name)
        (let [r (store {:index test-index-name
                        :documents docs
                        :number-of-shards 3})
              r1 (store {:index test-index-name
                         :force-merge 1
                         :documents docs
                         :number-of-shards 3})
              q (search {:index test-index-name
                         :must-refresh true
                         :query {:term {:field "name", :value "jack"}}})]
          (is (= 1000 (:total q)))
          (is (= 3 (count r)))
          (let [rc0 (into [] (for [item r]
                               (for [[k v] item]
                                 (:attempt-to-write v))))
                rc1 (into [] (for [item r1]
                               (for [[k v] item]
                                 (:attempt-to-write v))))
                m0 (into {} (for [[item] rc0] [item true]))
                m1 (into {} (for [[item] rc1] [item true]))]
            (is (= m0 m1)))

          (doseq [item r]
            (doseq [[k v] item]
              (is (> (:attempt-to-write v) 300))))))))
    
  (testing "cleanup-after"
    (delete-all test-index-name)))
