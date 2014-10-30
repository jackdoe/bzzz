(ns bzzz.core-handler-test
  (:import (java.io StringReader))
  (:require [clojure.data.json :as json])
  (:require [org.httpkit.client :as http-client])
  (:use clojure.test
        bzzz.core
        bzzz.const
        bzzz.util
        ring.adapter.jetty
        bzzz.index))

(def test-index-name :__lein-testing-handler-index)
(def host "http://localhost:3000/")
(def id default-identifier)
(def hosts [id host id id [id id [id id id host [host host id host] [host] [host id] [id host] id id id id id id [id] [id id [id]]]]])
(def hosts-bad [id host id "bzzz-testing-foofoo" id [id id [id id "bzz-testing-foobar" id host [host host "bzzz-testing-barfoo" id host] [host] [host id] [id host] id id id id id id [id] [id id [id "bzzz-testing-barbar"]]]]])
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

(defn put-request [enforce-limit size facet-size h can-return-partial]
  {:index test-index-name
   :can-return-partial can-return-partial
   :hosts h
   :query query
   :size size
   :timeout 10000
   :enforce-limits enforce-limit
   :facets {:name {:size facet-size}}})

(defn send-put-request 
  ([enforce-limit size facet-size] (send-put-request enforce-limit size facet-size hosts false))
  ([enforce-limit size facet-size h can-return-partial]
     (let [{:keys [status headers body error] :as resp}
           @(http-client/put host {:body (json/write-str (put-request enforce-limit size facet-size h can-return-partial))})]
       (jr body))))

(defn send-get-request [size facet-size]
  (let [{:keys [status headers body error] :as resp}
        @(http-client/get host {:body (json/write-str (put-request false size facet-size hosts false))})]
    (jr body)))

(defn send-delete-request []
  (let [{:keys [status headers body error] :as resp}
        @(http-client/delete host {:body (json/write-str delete-request)})]
    (jr body)))

(defonce server
  (run-jetty handler {:port 3000
                      :join? false}))

(deftest handle-test
  (testing "start"
    (.start server))

  (testing "discover"
    (discover))

  (testing "delete"
    (is (= (send-delete-request)
           {test-index-name "name:doe"})))

  (testing "store"
    (let [{:keys [status headers body error] :as resp}
          @(http-client/post host {:body (json/write-str store-request)})]
      (jr body)))

  (testing "put-partial"
    (refresh-search-managers)
    (dotimes [n 4]
      (let [should-be (+ 1 n)
            r (send-put-request true should-be 10 hosts-bad true)
            cnt (* 4 (count (flatten hosts)))]
        (println r)
        (is (not (= -1 (.indexOf ^String (:exception (send-put-request true should-be 10 hosts-bad false)) "Throwable java.lang.IllegalArgumentException: host is null"))))
        (is (= (count (:failed r)) 4))
        (is (= cnt (:total r)))
        (is (= should-be (count (:hits r)))))))

  ;; "Elapsed time: 27907.134538 msecs" - async with httpkit
  ;; "Elapsed time: 83890.603676 msecs" - sync request with async/go per host
  ;; (testing "n-times-put"
  ;;  (time (dotimes [n 1000]
  ;;           (send-put-request false 1000 10))))

  (testing "put"
    (refresh-search-managers)
    (let [r (send-put-request false 1000 10)
          r1 (send-put-request false 1 10)
          r2 (send-put-request true 1 2)
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
            r (send-get-request should-be should-be)
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
            r (send-put-request true should-be 10)
            cnt (* 4 (count (flatten hosts)))]
        (is (= cnt (:total r)))
        (is (= should-be (count (:hits r)))))))

  (testing "delete-and-should-be-zero"
    (is (= (send-delete-request)
           {test-index-name "name:doe"}))
    (refresh-search-managers)
    (let [r (send-put-request false 10 10)]
      (is (= 0 (:total r)))))
  
  (testing "stop"
    (.stop server)))
