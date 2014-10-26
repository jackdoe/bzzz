(ns bzzz.core-handler-test
  (:import (java.io StringReader))
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as http-client])
  (:use clojure.test
        bzzz.core
        bzzz.const
        ring.adapter.jetty
        bzzz.index))

(def test-index-name :testing-handler-index)
(def host "http://localhost:3000/")
(def id default-identifier)
(def hosts [id host id id [id id [id id id host [host host id host] [host] [host id] [id host] id id id id id id [id] [id id [id]]]]])

(def query {:term {:field "name"
                   :value "doe"}})

(def store-request
  {:index test-index-name
   :documents [{:name "jack doe"}
               {:name "jack doe"}
               {:name "john doe"}
               {:name "joe doe"}]
   :facets {:name {}}})

(def delete-request
  {:index test-index-name
   :query query})

(defn put-request [enforce-limit size facet-size]
  {:index test-index-name
   :hosts hosts
   :query query
   :size size
   :enforce-limits enforce-limit
   :facets {:name {:size facet-size}}})

(defn send-put-request [enforce-limit size facet-size]
  (http-client/put host {:accept :json
                         :as :json
                         :body (json/write-str (put-request enforce-limit size facet-size))}))

(defn send-get-request [size facet-size]
  (http-client/get host {:accept :json
                         :as :json
                         :body (json/write-str (put-request false size facet-size))}))

(defn send-delete-request []
  (http-client/delete host {:accept :json
                            :as :json
                            :body (json/write-str delete-request)}))
(defonce server
  (run-jetty handler {:port 3000
                      :join? false}))

(deftest handle-test
  (testing "start"
    (.start server))

  (testing "discover"
    (discover))

  (testing "delete"
    (is (= (:body (send-delete-request))
           {test-index-name "name:doe"})))

  (testing "store"
    (is (= (:body (http-client/post host {:accept :json
                                          :as :json
                                          :body (json/write-str store-request)}))
           {test-index-name true})))

  (testing "put"
    (refresh-search-managers)
    (let [r (:body (send-put-request false 1000 10))
          r1 (:body (send-put-request false 1 10))
          r2 (:body (send-put-request true 1 2))
          hcnt (count (flatten hosts))
          cnt (* 4 hcnt)
          nf (get-in r1 [:facets :name])
          ns (get-in r2 [:facets :name])]
      (is (= 2 (count ns)))
      (is (= (* 2 hcnt) (:count (first nf))))
      (is (= hcnt (:count (last nf))))
      (is (= "jack doe" (:label (first nf))))
      (is (> (:count (first nf)) (:count (last nf))))
      (is (= cnt (:total r1)))
      (is (= hcnt (count (:hits r1))))
      (is (= cnt (:total r)))
      (is (= cnt (count (:hits r))))))

  (testing "get"
    (refresh-search-managers)
    (dotimes [n 4]
      (let [should-be (+ 1 n)
            r (:body (send-get-request should-be should-be))
            nf (get-in r [:facets :name])]
        (is (= 4 (:total r)))
        (is (= 2 (:count (first nf))))
        (is (= "jack doe" (:label (first nf))))
        (if (> (count nf) 1)
          (is (> (:count (first nf)) (:count (last nf)))))
        (is (= should-be (count (:hits r)))))))

  (testing "put-limit"
    (refresh-search-managers)
    (dotimes [n (* 4 (count (flatten hosts)))]
      (let [should-be (+ 1 n)
            r (:body (send-put-request true should-be 10))
            cnt (* 4 (count (flatten hosts)))]
        (is (= cnt (:total r)))
        (is (= should-be (count (:hits r)))))))

  (testing "delete-and-should-be-zero"
    (is (= (:body (send-delete-request)
                  {test-index-name "name:doe"})))
    (refresh-search-managers)
    (let [r (:body (send-put-request false 10 10))]
      (is (= 0 (:total r)))))
  
  (testing "stop"
    (.stop server)))
