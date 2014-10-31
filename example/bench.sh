#!/bin/sh
curl -XPOST -d '{
    "documents": [
        { "name": "john doe" },
        { "name": "jack doe" }
    ],
    "facets": {"name":{}}
    "index": "bzzz-bench"
}' http://localhost:3000/

curl -XGET -d '{
    "query": "name:doe"
    "facets": {"name":{}}
    "index": "bzzz-bench"
}' http://localhost:3000/

boom -n 100000 -c 20 -m GET -d '{
    "query": "name:doe"
    "facets": {"name":{}}
    "index": "bzzz-bench"
}' http://localhost:3000/
