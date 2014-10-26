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
               {:name "john doe"}]
   :facets {:name {}}})

(def delete-request
  {:index test-index-name
   :query query})

(defn put-request [enforce-limit size]
  {:index test-index-name
   :hosts hosts
   :query query
   :size size
   :enforce-limits enforce-limit
   :facets {:name {:size 7}}})

(defn send-put-request [enforce-limit size]
  (http-client/put host {:accept :json
                         :as :json
                         :body (json/write-str (put-request enforce-limit size))}))

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
    (let [r (:body (send-put-request false 1000))
          r1 (:body (send-put-request false 1))
          hcnt (count (flatten hosts))
          cnt (* 2 hcnt)]
      (is (= cnt (:total r1)))
      (is (= hcnt (count (:hits r1))))
      (is (= cnt (:total r)))
      (is (= cnt (count (:hits r))))))

  (testing "put-limit"
    (refresh-search-managers)
    (dotimes [n (* 2 (count (flatten hosts)))]
      (let [should-be (+ 1 n)
            r (:body (send-put-request true should-be))
            cnt (* 2 (count (flatten hosts)))]
        (is (= cnt (:total r)))
        (is (= should-be (count (:hits r)))))))

  (testing "delete-and-should-be-zero"
    (is (= (:body (send-delete-request)
                  {test-index-name "name:doe"})))
    (refresh-search-managers)
    (let [r (:body (send-put-request false 10))]
      (is (= 0 (:total r)))))
  
  (testing "stop"
    (.stop server)))
