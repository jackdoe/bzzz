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
    "query": {"term-payload-clj-score": {
    "field": "name",
    "value": "doe",
    "clj-eval": "
     (fn [payload ^java.util.Map local-state fc doc-id]
;;       (.get local-state doc-id)
;;       (.put local-state doc-id payload)
       (float payload))
"}},
    "index": "bzzz-bench"
}' http://localhost:3000/ | json_xs | grep took

curl -XGET -d '{
    "query": {"term-payload-clj-score": {
    "field": "name",
    "value": "doe",
    "clj-eval": "
     (fn [payload ^java.util.Map local-state fc doc-id]
       (.get local-state \"a\")
       (.put local-state \"a\" payload)
       (float payload)))
"}},
    "index": "bzzz-bench"
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
     (fn [payload ^java.util.Map local-state fc doc-id]
       (.get local-state doc-id)
       (.put local-state doc-id payload)
       (float payload))
"}},
    "index": "bzzz-bench"
}' http://localhost:3000/

boom -n 1000 -c 10 -m GET -d '
{
    "query": {"term-payload-clj-score": {
    "field": "name",
    "value": "doe",
    "clj-eval": "
     (fn [payload ^java.util.Map local-state fc doc-id]
;;       (.get local-state \"something\")
;;       (.put local-state \"something\" payload)
       (float payload))
"}},
    "index": "bzzz-bench"
}' http://localhost:3000/
