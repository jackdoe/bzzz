# bzzz

i have no idea what i'm doing
===

clojure + lucene (4.9 at the moment) + jetty + ring

looked at https://github.com/weavejester/clucy/blob/master/src/clucy/core.clj for inspiration


```
$ lein run -- --port 3000 --directory /tmp/bzbzbz # by default 3000 and /tmp/BZZZ

$ curl -XPOST http://localhost:3000/ -d '{"index":"bzbz","documents":[{"name_store_index":"johny doe"}, {"name_store_index":"jack doe"}]}'
$ curl -XGET http://localhost:3000/ -d '{"index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'
$ curl -XPUT http://localhost:3000/ -d '{"hosts":[["http://localhost:3000","http://localhost:3000"],"http://localhost:3000/","http://localhost:3000"], "index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'
```

how it works
===

* starts a web server on port 3000
* POST requests are stored (expects json array of hashes [ {"key":"value"} ])
* GET requests are searches
* PUT requests are doing remote searches
* uses one `atom` hash that contains { "index_name": SearcherManager }
* the SearcherManagers are refreshed every 5 seconds (so the writes will be searchable after max 5 seconds)

POST
====

```
{
  "index":"index_name"
  "documents": [ { "name_store_index": "jack","id":"1230812903" } ]
}

```

* if the field name contains _store it will be Field.Store.YES typed
* if the field name contains _index it will be Field.Index.ANALYZED with WhitespaceAnalyzer
* every POST requests can create/append to existing index, and it will optimize it to 1 segment
* if the document contains the key "id" it will try to update new Term("id",document.get("id"))
* keep in mind that "id" is nothing special, it will is just used to overwrite existing documents, and for deletion (still it will be doing delete-by-query)

GET
====

```
{
  "index":"index_name"
  "query":"name_store_index:jack",
  "size":5
}
```

will run the Lucene's QueryParser generated query from the "query" key against the "index_name" index using the WhitespaceAnalyzer

PUT
===
```
{
  "index":"index_name"
  "query":"name_store_index:jack",
  "size":5,
  "hosts":["http://localhost:3000","http://localhost:3000"]
}
curl -XPUT http://localhost:3000/ -d '{"hosts":[["http://localhost:3000/","http://localhost:3000"],"http://localhost:3000","http://127.0.0.1:3000"], "split":2, "index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'


original request:
:put {:hosts [[http://localhost:3000/ http://localhost:3000] http://localhost:3000 http://127.0.0.1:3000], :split 2, :index bzbz, :query name_store_index:johny AND name_store_index:doe, :size 10}
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

TODO
===

* DELETE
* simple replication, or maybe use zookeeper and sanity "query" that can decide if there is a need for copy or not


## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
