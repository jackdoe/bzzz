# bzzz

i have no idea what i'm doing
===

clojure + lucene + jetty + ring

looked at https://github.com/weavejester/clucy/blob/master/src/clucy/core.clj for inspiration


```
$ lein run

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
  "documents": [ { "name_store_index": "jack" } ]
}

```

* if the field name contains _store it will be Field.Store.YES typed
* if the field name contains _index it will be Field.Index.ANALYZED with WhitespaceAnalyzer
* every POST requests can create/append to existing index, and it will optimize it to 1 segment
* if the document contains the key "id" it will try to update new Term("id",document.get("id"))

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

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
