#!/bin/sh
curl -XPOST -d '{
    "documents": [
        { "name": "john|1 doe|2147483647" },
        { "name": "jack|2147483647 doe|1" },
        { "name": "john|1 doe|2147483647" },
        { "name": "jack|2147483647 doe|1" }
    ],
    "analyzer":{"name":{"type":"custom","tokenizer":"whitespace","filter":[{"type":"delimited-payload"}]}},
    "facets": {"name":{}}
    "index": "bzzz-bench"
}' http://localhost:3000/

curl -XGET -d '{
    "query": "name:doe"
    "index": "bzzz-bench"
}' http://localhost:3000/ | json_xs | grep took


curl -XGET -d '{
    "explain": false,
    "query": {"term-payload-clj-score": {
    "fixed-bucket-aggregation": [{"name":"bzbzbzbz","buckets": 10}],
    "field": "name",
    "value": "doe",
    "clj-eval": "
     (fn [^bzzz.java.query.ExpressionContext ctx]
       (let [payload (.payload_get_int ctx)]
           (.fba_aggregate_into_bucket ctx 0 (mod (.docID ctx) 10))
;;         (.local_state_get ctx (.docID ctx))
;;         (.local_state_set ctx (.docID ctx) payload)
         (float payload)))
"}},
    "index": "bzzz-sharded-bench"
}' http://localhost:3000/ | json_xs | grep took

boom -n 100000 -c 20 -m GET -d '{
    "query": "name:doe"
    "facets": {"name":{}}
    "index": "bzzz-bench"
}' http://localhost:3000/


boom -n 100000 -c 20 -m GET -d '
{
    "query": {"term-payload-clj-score": {
    "field": "name",
    "value": "doe",
    "clj-eval": "
     (fn [^bzzz.java.query.ExpressionContext ctx]
       (let [payload (.payload_get_int ctx)]
         (.local-state_get ctx (.docID ctx))
         (.local-state_set ctx (.docID ctx) payload)
         (float payload)))
"}},
    "index": "bzzz-bench"
}' http://localhost:3000/

boom -n 1000000 -c 10 -m GET -d '
{
    "query": {"term-payload-clj-score": {
    "field": "name",
    "value": "doe",
    "clj-eval": "
     (fn [^bzzz.java.query.ExpressionContext ctx]
       (let [payload (.payload_get_int ctx)]
         (float payload)))
"}},
    "index": "bzzz2-bench-5m"
}' http://localhost:3000/
