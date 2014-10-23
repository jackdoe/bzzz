# bzzz

*work in progress* stateless (clojure + lucene-4.9.1 + jetty + ring) search service

## run
```
$ lein trampoline run -- --directory /tmp/bzbzbz
```

## store something

```
$ curl -XPOST http://localhost:3000/ -d '
{
    "documents": [
        { "name": "john doe" },
        { "name": "jack doe" }
    ],
    "index": "bzbz"
}'
```

## search it

```
$ curl -XGET http://localhost:3000/ -d '
{
    "index": "bzbz",
    "query": "name:doe"
}'
```

## \o/

### Analyzers - "You will find only what you bring in"

imagine simplified inverted index representation:

```
data = [ { name: jack doe }, { name: john doe }]
                 0                   1
inverted = {
  name: {
    jack: [0],
    john: [1],
    doe:  [0,1]
  }
}
```
queries are just operations on term-sets, for example `jack AND doe` is `([0] AND [0,1])` which results in `0: data[0] - { name: jack doe }`.

Analyzers stand between your data and the inverted index: `jack doe -> whitespaceTokenizer -> [jack] [doe]`, you you can also create something like a chain of token-modifiers/emitters: `input -> tokenizer -> tokenfilter -> tokenfilter...`

```
# example custom analyzer:
{ "type":"custom","tokenizer":"whitespace","filters":[{"type":"lowercase"}]}

Jack Doe
  -> whitespaceTokenizer ->
     [Jack]
        -> LowerCaseTokenFilter
           [jack]
     [Doe]
        -> LowerCaseTokenFilter
           [doe]
```

So using this token filter chain, when you give it a string `Jack Doe`, it will produce the terms `jack` and `doe`.
Lets say that your query is `Doe`, if you do `{"term":{"field":"name", "value":"Doe"}}`, Lucene will lookup all documents in the `inverted` index that have the term `Doe`, but your custom analyzer will never produce that term because it actually `lowercase`s all the input, of course if you look for `{"term":{"field":"name", "value":"doe"}}` it will work, because this is the term that was produced, and at index time the `Jack Doe` document's id was added to the list of document ids that have the term `doe` in them.

#### predefined

BZZZ supports some predefined analyzers like the `StandardAnalyzer`, `WhitespaceAnalyzer` and `KeywordAnalyzer`.

Example:

```
{
    "query": { "term":{ "field":"some_field_name","value":"joe" } }, (or indexing documents: [{"some_field_name":"joe doe"}])
    "analyzer": { "some_field_name": { "type": "standard" } }
}
```

`StandardAnalyzer` will be used used when indexing documents with that field, or doing query parsing using Lucene's `QueryParser`.

#### custom

There is also an potion to create your own token-filter-chain, using custom combination of `tokenizer + token filters + character filters`.

```
{
    "query": ... / "documents":[...]
    "analyzer": {
        "some_field_name": {
            "char-filter": [
                {
                    "pattern": "(?i)X+",
                    "replacement": "Z",
                    "type": "pattern-replace"
                }
            ],
            "filter": [
                {
                    "type": "lowercase"
                }
            ],
            "max_gram": 3,
            "min_gram": 2,
            "tokenizer": "ngram",
            "type": "custom"
        }
    }
}
```

with the search string "JackX", the custom analyzer will produce:

* ja
* jac
* ac
* ack
* ck
* ckz
* kz

Notice how all terms are `lowercased`, and the `X` in the end is replaced with `z`, also you can see that we made our regex case sensitive, because char-filters are executed before the tokenizer and the token filters.

If we use the same analyzer with lucene's QueryParser and search for 'jackx' it will look like this:

```
{
   "query": { "query-parser": { "query": "some_field_name:jack, "default-operator":"and" } },
   analyzer... (same sa above)
}

internally this will be turned into:
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "field": "some_field_name",
                        "value": "ja"
                    }
                },
                {
                    "term": {
                        "field": "some_field_name",
                        "value": "jac"
                    }
                }
                {
                    "term": {
                        "field": "some_field_name",
                        "value": "ac"
                    }
                }
                {
                    "term": {
                        "field": "some_field_name",
                        "value": "ack"
                    }
                }
            ]
        }
    }
}

```

in the end, everything is a term, if you know what your modifier emmits you can search for it.

At the moment I am working on adding more and more analyzers/tokenizers/tokenfilters/charfilters into BZZZ.


## why BZZZ

* have moderately useful (out of the box) network lucene wrapper
* distribute work
* should be able to restart frequently
* _user_ controlled sharding


BZZZ being extremely simple it can actually scale very well if you know your data, for example if we have 100_000_000 documents and we want to search them, we can just spawn 100BZZZ processess across multiple machines, and just put different pieces of data in different boxes (hash($id) % BOXES). BZZZ supports swarm like queries, so you can ask 1 box, to ask 5 boxes, and each of those 5 boxes can ask 5 more, and actually the _user_ is in control of that.

* multi-searching shard identification and automatic shard resolving
the way BZZZ does this is by simply continuously updating a @discover-hosts* atom map, and every time you a box checks if another box is alive, it also gets its current @discover-hosts* and it merges it, it also gets the box's identifier.

This identifier can be used when a query is construcded like so:
```
PUT:
{
    "hosts": [
        "__shard_0",
        "__shard_1"
    ],
    "index": "example",
    "query": "name:jack",
}
```
(of course instead of __shard_0/1 you can have `http://host.example.com:3000`, but everything you put in the hosts array, BZZZ will try to resolve from the @peers* map, and see if there are any boxes that were alive within the last `acceptable-discover-time-diff*` (by default 10) second and randomly picks one.

In the `hosts` array you can also add arrays like:
```
PUT:
{
    "hosts": [
        [
            "__shard_0",
            "__shard_1"
        ],
        [
            "__shard_2",
            "__shard_3"
        ]
    ]
}
```

so in this case, the host that you send the request to, will fire multi-search request to __shard0, and ask it for `["__shard_0","__shard_1"]` and another multi-search reques to __shard_2 and ask it to `["__shard_2","__shard_3"]`, so in the end you will query 1 box, which will query 2 boxes, and each of those will query 2 boxes (including themselves).

This can can become quite hard to grasp `["a","b",["c","d","e",["f,"g","h",["z","x","c"]]]]` for example.

All those examples with `hosts` keys are actually a `mutli-search` requests, and the only difference between a regular search request and a multi-search request is the fact that it is sent using the `HTTP PUT` method, and has a `hosts` key

* you specify the identifier of a process by using the `--identifier` startup parameter, there is _no_ disk stored state for it, so you can switch processess that were serving identifier `a` to start serving `b` (of course you have to be careful if the shard served from A is also the same as B, or just change the --directory of the processto read from shard A's data)


## state / schema / mappings

one very important difference between BZZZ and other Lucene frontends (like ElasticSearch or Solr) is that BZZZ
does not keep a schema-like mapping anywhere, every index request/query request can have different analyzer mapping
and the actual field name relates to the store/indextype

if you want to have field named `name`:

* name -> indexed analyzed with norms and stored
* name_no_store -> indexed analyzed with norms, not stored
* name_not_analyzed -> indexed not analyzed with norms, stored
* ... you can see where this is going
* no_store
* no_index
* no_norms
* not_analyzed
* and you can combine them.

so *any* BZZZ process can just start reading/writing into any index, without a care in the world. you can also just copy a bunch of lucene files around, and everything will work, this convension is also very easy to be copied in java, so you can easilly share same index with different lucene writer.

## directories / writers / searchers

### IndexWriter

_every_ write request to BZZZ does the following:

* _open_ a new Directory in the `--directory`/`index_name` path
* write the data
* optimize the lucene index
* close the writer

Lucene's NIOFSDirectory does file based locking, so you can write to the same files from multiple entry points

### IndexSearcher/SearcherManager

on every _query_ request comes, BZZZ will check if there is already a `SearcherManager` servicing this index, if not it will create new SearcherManager, and then acquire() an IndexSearcher for the current request

_every_ 5 seconds all SearcherManagers are asked to refresh if needed (if data changed for example)

so when BZZZ starts it actually does not know which indexes it has in the root directory, it just waits for the first request to see if it can find the requested index or not.

---------------------


# RAMBLINGS, will be rewritten..
some more examples.. and random ramblings.
```
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
            "type": "whitespace"
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
            "type": "standard"
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

analyzers
===

currently there is partial support for custom analyzers like:
```
{
    "field": {
        "char-filter": [
            {
                "pattern": "X+",
                "replacement": "ZZ",
                "type": "pattern-replace"
            },
            {
                "type": "html-strip"
            }
        ],
        "tokenizer": "keyword",
        "type": "custom"
    }
}

```
predefined analyzers
---
* whitespace `type: "whitespace"`
* standard `type: "standard"`
* keyword `type: "keyword"`
* .. work in progress

custom tokenfilter chain
===

tokenizers
---
* whitespace * keyword `type: custom, tokenizer: "whitespace"` from token `test foo` produces `test` `foo`
* keyword `type: custom, tokenizer: "keyword"` from token `test foo` produces `test foo`
test
* ngram `type: custom, min_gram 1, max_gram 30, tokenizer: "ngram"` from token `test` produces `t` `te` `tes` `test` `e` `es` `est` `s` `st` `t`
* edge-ngram `type: custom, min_gram 1, max_gram 30, tokenizer: "edge-ngram"` from token `test` produces `t` `te` `tes` `test`
* .. work in progress

char-filters
---
* pattern-replace `type: "pattern-replace", pattern: "ABC", "replacement": "ZZZ"`
* html-strip `type: "html-strip", escaped-tags:["br"]` from `bzbz<br><btml>` produces `bzbz<br>`
* .. work in progress

token-filters
---
* .. work in progress

auto identifier host round robbin
===

if you add `--identifier "something-that-identifies-the-data-for-this-process"` and `--hosts=host_a:port,host_b:port...` `bzzz` will automatically query those hosts for their identifier and you can just send queries with `"hosts:["identifier_a","identifier_b"]"` and it will query random hosts who have < 10 second stamp in the last probe for those specific identifiers

thanks to
===
* stackoverflow
* looked at https://github.com/weavejester/clucy/blob/master/src/clucy/core.clj for inspiration
* http://lucene.apache.org/core/4_9_1/core/overview-summary.html
* http://clojuredocs.org/

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
