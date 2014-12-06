(ns bzzz.core-handler-test
  (:import (java.io StringReader))
  (:require [clojure.data.json :as json])
  (:require [org.httpkit.client :as http-client])
  (:require [bzzz.query :as query])
  (:use clojure.test
        bzzz.core
        bzzz.const
        bzzz.util
        ring.adapter.jetty
        bzzz.index-directory
        bzzz.index-search))

(def test-index-name :__lein-testing-handler-index)

(def host "http://localhost:3000/")
(def id default-identifier)
(def hosts [id host id id [id id [id id id host [host host id host] [host] [host id] [id host] id id id id id id [id] [id id [id]]]]])
(def hosts-bad [id host id "bzzz-testing-foofoo" id [id id [id id "bzzz-testing-foobar" id host [host host "bzzz-testing-barfoo" id host] [host] [host id] [id host] id id id id id id [id] [id id [id "bzzz-testing-barbar"]]]]])
(def query {:term {:field "name"
                   :value "doe"}})

(def store-request
  {:index test-index-name
   :force-merge 1
   :analyzer {:name_payload {:type "custom"
                             :tokenizer "whitespace"
                             :filter [{:type "delimited-payload"
                                       :delimiter "|"}]}}
   :documents [{:name "jack doe" :popularity_double 300000.281 :name_payload "xxxyyy|1000"}
               {:name "jack doe" :popularity_double 30000.284 :name_payload "xxxyyy|2000"}
               {:name "john doe" :popularity_double 3000.283 :name_payload "xxxyyy|3000"}
               {:name "joe doe doe" :popularity_double 1000.281 :name_payload "xxxyyy|4000"}]
   :facets {:name {}
            :name_payload {:use-analyzer "name_payload"}}})

(def delete-request
  {:index test-index-name
   :query query})

(defn put-request [enforce-limit size facet-size h can-return-partial]
  {:index test-index-name
   :can-return-partial can-return-partial
   :hosts h
   :sort {:expression "sqrt(_score) + ln(popularity_double)"
          :bindings ["popularity_double"]}
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

(defn send-generic-get-request [request]
  (let [{:keys [status headers body error] :as resp}
        @(http-client/get host {:body (json/write-str request)})]
    (jr body)))

(defn send-get-request [size facet-size]
  (send-generic-get-request (put-request false size facet-size hosts false)))

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
      (is (= (* 2 hcnt) (:count (first nf))))
      (is (= 2 (count ns)))
      (is (= (* 2 hcnt) (:count (first ns))))
      (is (= hcnt (:count (last nf))))
      (is (= "jack doe" (:label (first nf))))
      (is (> (:count (first nf)) (:count (last nf))))
      (is (= cnt (:total r1)))
      (is (= hcnt (count (:hits r1))))
      (is (= (:popularity_double (first (:hits r1))) "300000.281"))
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

  (testing "get-check-sort"
    (refresh-search-managers)
    (let [r (send-get-request 5 5)
          nf (get-in r [:facets :name])]
      (is (< (:_score (first (:hits r))) (:_score (last (:hits r)))))
      (is (= (:popularity_double (first (:hits r))) "300000.281"))
      (is (= 4 (:total r)))))

  (testing "put-limit"
    (refresh-search-managers)
    (dotimes [n (* 4 (count (flatten hosts)))]
      (let [should-be (+ 1 n)
            r (send-put-request true should-be 10)
            cnt (* 4 (count (flatten hosts)))]
        (is (= cnt (:total r)))
        (is (= should-be (count (:hits r)))))))

  (testing "broken-payload"
    (let [broken-query {:term-payload-clj-score {:field "name_payload", :value "xxxyyy"
                                                 :field-cache ["some_integer"]
                                                 :clj-eval "
(fn [payload local-state fc doc-id]
  (+ 10
     non-existing-thing
     payload
     (.get ^org.apache.lucene.search.FieldCache$Ints (:some_integer fc) doc-id)))
"
                                                 }}
          good-query {:term-payload-clj-score {:field "name_payload", :value "xxxyyy"
                                               :field-cache ["some_integer"]
                                               :clj-eval "
(fn [payload local-state fc doc-id]
  (+ 10
     payload
     (.get ^org.apache.lucene.search.FieldCache$Ints (:some_integer fc) doc-id)))
"
                                               }}
          req-good {:index test-index-name
                    :explain true
                    :query good-query}
          req-bad {:index test-index-name
                   :explain true
                   :query broken-query}]
      (reset! query/allow-unsafe-queries* false)
      (is (substring? "--allow-unsafe-queries" (:exception (send-generic-get-request req-good))))
      (reset! query/allow-unsafe-queries* true)
      (is (nil? (:exception (send-generic-get-request req-good))))
      (let [r0 (send-generic-get-request req-bad)
            r1 (send-generic-get-request req-good)]
        (is (= 4 (:total r1)))
        (is (= 4010.0 (:_score (nth (:hits r1) 0))))
        (is (= 3010.0 (:_score (nth (:hits r1) 1))))
        (is (= 2010.0 (:_score (nth (:hits r1) 2))))
        (is (= 1010.0 (:_score (nth (:hits r1) 3))))
        (is (substring? "non-existing-thing in this context" (:exception r0))))))


  (testing "delete-and-should-be-zero"
    (is (= (send-delete-request)
           {test-index-name "name:doe"}))
    (refresh-search-managers)
    (let [r (send-put-request false 10 10)]
      (is (= 0 (:total r)))))

  (testing "create-alias"
    (let [aliased "core-handler-test-alias-testing"
          r (fn [fn x] (json/read-str (:body @(fn "http://localhost:3000" {:body (json/write-str x)}))))
          g (fn [x] (dissoc (r http-client/get {:index x
                                                :query "name:alias"}) "took"))]
      (is (= 0 (get (g aliased) "total")))
      (r http-client/post {:index test-index-name :alias-set aliased})
      (r http-client/post {:index aliased :documents [{:name "alias write"}]})
      (refresh-search-managers)
      (let [ra (g aliased)
            rb (g test-index-name)]
        (is (= ra rb))
        (is (= 1 (get ra "total"))))
      (r http-client/post {:index test-index-name :alias-del aliased})
      (is (= 0 (get (g aliased) "total")))
      (r http-client/post {:index test-index-name :alias-set aliased})
      (is (= 1 (get (g aliased) "total")))
      (r http-client/delete {:index aliased :query {:match-all {}}})
      (refresh-search-managers)
      (let [ra (g aliased)
            rb (g test-index-name)]
        (is (= ra rb))
        (is (= 0 (get ra "total"))))))


  (testing "stop"
    (.stop server)))
