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

  (testing "store-eval"
    (let [v (kv/store {:file-name "test-db" :lock-name "test-db-lock" :args [1,2,3] :clj-eval "
(fn [^org.mapdb.DB db args]
  (let [m (.getHashMap db \"hello\")]
    (.remove m \"hello\")
    (.put m \"hello\" \"world\")
    (.put m \"hello-args\" args)
    (.commit db)
    true))"
                       })]
      (is (= v true))))

  (testing "search-eval"
    (let [v (kv/search {:file-name "test-db" :clj-eval "
(fn [^org.mapdb.DB db]
  (let [m (.getHashMap db \"hello\")]
    (.get m \"hello-args\")))
"})]
      (is (= v [1 2 3]))))

  (testing "search"
    (let [v (kv/search {:file-name "test-db" :obj-name "hello"})]
      (is (= "world" (get v "hello"))))))
