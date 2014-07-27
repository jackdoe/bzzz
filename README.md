# bzzz

proof of concept, i have no idea what i'm doing
===

clojure + lucene + jetty + ring

looked at https://github.com/weavejester/clucy/blob/master/src/clucy/core.clj for inspiration


```
$ lein run
curl -XPOST http://localhost:3000/ -d '{"index":"bzbz","documents":[{"name_store_index":"johny doe"}, {"name_store_index":"jack doe"}]}'
curl -XGET http://localhost:3000/ -d '{"index":"bzbz","query":"name_store_index:johny AND name_store_index:doe","size":10}'

```

## License

Copyright © 2014 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
