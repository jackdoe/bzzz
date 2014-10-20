# bzzz

*work in progress* stateless (clojure + lucene (4.9.1 at the moment) + jetty + ring) search service

looked at https://github.com/weavejester/clucy/blob/master/src/clucy/core.clj for inspiration



```
$ lein trampoline run -- --port 3000 --directory /tmp/bzbzbz # by default 3000 and /tmp/BZZZ

$ curl -XPOST http://localhost:3000/ -d '
{
    "documents": [
        {
            "name_store_index": "johny doe"
        },
        {
            "name_store_index": "jack doe"
        }
    ],
    "index": "bzbz"
}'
$ curl -XGET http://localhost:3000/ -d '
{
    "index": "bzbz",
    "query": "name_store_index:johny AND name_store_index:doe",
    "size": 10
}'
$ curl -XPUT http://localhost:3000/ -d '{
    "hosts": [
        [
            "http://localhost:3000",
            "http://localhost:3000"
        ],
        "http://localhost:3000/",
        "http://localhost:3000"
    ],
    "index": "bzbz",
    "query": "name_store_index:johny AND name_store_index:doe",
    "size": 10
}
'
$ curl -XDELETE http://localhost:3000/ -d '{
    "index": "bzbz",
    "query": "name_store_index:johny AND name_store_index:doe"
}
'
# curl -XGET http://localhost:3000/ -d '{
    "index": "bzbz",
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "field": "name_store_index",
                        "value": "johny"
                    }
                }
            ]
        }
    },
    "size": 10
}
'
```

how it works
===

* starts a web server on port 3000
* POST requests are stored (expects json array of hashes `[ {"key":"value"} ]`)
* GET requests are searches
* PUT requests are doing remote searches
* DELETE requests delete by query
* uses one `atom` hash that contains `{ "index_name": SearcherManager }`
* the `SearcherManagers` are refreshed every 5 seconds (so the writes/deletes will be searchable after max 5 seconds)

compile and start standalone
===

```
$ lein test
$ lein uberjar
$ java -jar target/bzzz-0.1.0-SNAPSHOT-standalone.jar
```

POST
====

```
{
    "analyzer": {
        "name_store_index": {
            "use": "whitespace"
        }
    },
    "documents": [
        {
            "id": "1230812903",
            "name_store_index": "jack"
        }
    ],
    "index": "index_name"
}
```

* if the field name contains _store it will be Field.Store.YES typed
* if the field name contains _index it will be Field.Index.ANALYZED
* if the field name contains _no_norms it will be Field.Index.(ANALYZED|NOT_ANALYZED)_NO_NORMS
* every POST requests can create/append to existing index, and it will optimize it to 1 segment
* if the document contains the key "id" it will try to update new Term("id",document.get("id"))
* keep in mind that "id" is nothing special, it will is just used to overwrite existing documents, and for deletion (still it will be doing delete-by-query)
* every store does commit() and forceMerge(1)
* per field analyzers are supported for every index request (by default it is id: keyword, everything else whitespace). there is no state for the per-field-analyzers, the "query-parser" query supports per-request based per-field-analyzers


GET
====

```
{
    "analyzer": {
        "name_store_index": {
            "use": "standard"
        }
    },
    "explain": false,
    "index": "index_name",
    "page": 0,
    "query": {
        "query-parser": {
            "query": "name_store_index:jack@jack"
        }
    },
    "size": 5
}
```
will run the Lucene's QueryParser generated query from the "query" key against the "index_name" index using the Standard analyzer.


```
{
  "index":"bzbz",
  "query": {
               "bool": {
                   "must":[
                       {
                           "term":{
                              "field":"name_store_index",
                              "value":"johny"
                           }
                       }
                   ]
               }
           },
  "size":10
}
```
There is also minimal support for BooleanQuery and TermQuery, like the example above.

DELETE
===
```
{
  "index":"index_name"
  "query":"name_store_index:jack",
}
```
will delete all documents matching the query, if field name is not specified it defaults to "id"
every delete does commit() and forceMerge(1)

PUT
===
```
{
    "hosts": [
        [
            "http://localhost:3000/",
            "http://localhost:3000"
        ],
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    "index": "index_name",
    "query": "name_store_index:jack",
    "size": 5
}
curl -XPUT http://localhost:3000/ -d '{"hosts":[["http://localhost:3000/","http://localhost:3000"],"http://localhost:3000","http://127.0.0.1:3000"], "index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'


original request:
:put {:hosts [[http://localhost:3000/ http://localhost:3000] http://localhost:3000 http://127.0.0.1:3000], :index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10}
searching < name_store_index:johny AND name_store_index:doe > on index < bzbz > with limit < 10 > in part < [http://127.0.0.1:3000] >
searching < name_store_index:johny AND name_store_index:doe > on index < bzbz > with limit < 10 > in part < [http://localhost:3000] >

spawning thread (that also does multi-search):
searching < name_store_index:johny AND name_store_index:doe > on index < bzbz > with limit < 10 > in part < [http://localhost:3000/ http://localhost:3000] >
the internal-multi-search-request for the list in the hosts list in the initial request:
:put {:index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10, :hosts [http://localhost:3000/ http://localhost:3000]}
searching < name_store_index:johny AND name_store_index:doe > on index < bzbz > with limit < 10 > in part < [http://localhost:3000/] >
:get {:index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10, :hosts [http://127.0.0.1:3000]}
:get {:index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10, :hosts [http://localhost:3000]}
searching < name_store_index:johny AND name_store_index:doe > on index < bzbz > with limit < 10 > in part < [http://localhost:3000] >
:get {:index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10, :hosts [http://localhost:3000/]}
:get {:index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10, :hosts [http://localhost:3000]}

```

SPAM
---

```
[
    [
        "http://localhost:3000/",
        "http://localhost:3000"
    ],
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]
```

it basically spawns 3 threads
* 1 for ["http://localhost:3000/","http://localhost:3000"]
* 1 for "http://localhost:3000"
* 1 for "http://localhost:3000"

and of course `["http://localhost:3000/","http://localhost:3000"]` sends `PUT { ... "hosts": ["http://localhost:3000/","http://localhost:3000"]}` which also spawns 2 threads (one per host).

another example would be:

```
[
   ["a","b",["c","d","e",["f,"g"]]],
   "h",
   "i"
]

or 
curl -XPUT http://localhost:3000/ -d '{"hosts":[["http://localhost:3000/","http://localhost:3000",["http://localhost:3000","http://127.0.0.1:3000","http://localhost:3000","http://127.0.0.1:3000"]],"http://localhost:3000","http://127.0.0.1:3000"], "index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'

[
    [
        "http://localhost:3000/",
        "http://localhost:3000",
        [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://127.0.0.1:3000"
        ]
    ],
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]
```

STAT
===
`/_stat` is a hackish endpoint that will trigger core/stat so, `http://localhost:3000/_stat` will return something like:

```
{
    "analyzer": "PerFieldAnalyzerWrapper({\"id\" #<KeywordAnalyzer org.apache.lucene.analysis.core.KeywordAnalyzer@6e306ef7>}, default=org.apache.lucene.analysis.core.WhitespaceAnalyzer@53d27789)",
    "index": {
        "bzbz": {
            "docs": 2,
            "has-deletions": false
        },
        "lein-test-testing-index": {
            "docs": 0,
            "has-deletions": false
        },
        "testing-index": {
            "docs": 0,
            "has-deletions": false
        }
    }
}

```
## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
