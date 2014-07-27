# bzzz

i have no idea what i'm doing
===

clojure + lucene (4.9 at the moment) + jetty + ring

looked at https://github.com/weavejester/clucy/blob/master/src/clucy/core.clj for inspiration


```
$ lein run -- --port 3000 --directory /tmp/bzbzbz # by default 3000 and /tmp/BZZZ

$ curl -XPOST http://localhost:3000/ -d '{"index":"bzbz","documents":[{"name_store_index":"johny doe"}, {"name_store_index":"jack doe"}]}'
$ curl -XGET http://localhost:3000/ -d '{"index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'

```

how it works
===

* starts a web server on port 3000
* POST requests are stored (expects json array of hashes [ {"key":"value"} ])
* GET requests are searches
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


WIP
===

working towards first stab at very very simple state-spam, that we can use to query multiple boxes

* every 100ms each box spams its current mapping* content to udp broadcast to every port specified with --spam-ports=1234,12345, and it sends a map like:

```
{
   "index-name":123123
   "index-name2":182378912
}
```
the value is actualy number of documents matching an user-specified query (probably in many cases it will be matchAll)

* when a box receives a packet like that will update a z-state* atom to reflect the peer's ip address, its current mapping* and a timestamp, like:

```
{
  "index-name": {
       "127.0.0.1": {
             "stamp":23187239,
             "count":47636
       },
       "10.0.0.1": {
             "stamp":12388789
             "count":7443
       },
  }

```

* so when we receive a multi-query, we will just look for all hosts in the z-state that we have received update in the last second *AND* the number of documetns is the same as our mapping* value (or maybe only the max)

as you can see this is pretty barbaric approach, doesnt guarantee anything, same number of documents doesnt mean anything, also the user will be responsible for managing the documents in sync


TODO
===

* DELETE
* simple replication, or maybe use zookeeper and sanity "query" that can decide if there is a need for copy or not
* add support for multiple queries, and do topN merge



## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
