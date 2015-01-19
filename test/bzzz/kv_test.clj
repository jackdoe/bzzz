(ns bzzz.kv-test
  (:use clojure.test
        [bzzz.kv :as kv]))

(deftest test-kv
  (testing "store"
    (let [v (kv/store {:file-name "test-db" :lock-name "test-db-lock" :clj-eval "
(fn [^org.mapdb.DB db]
  (let [m (.getHashMap db \"hello\")]
    (.remove m \"hello\")
    (.put m \"hello\" \"world\")
    (.commit db)
    true))"
                       })]
      (is (= v true))))

  (testing "search"
    (let [v (kv/search {:file-name "test-db" :obj-name "hello"})]
      (is (= "world" (get v "hello"))))))
