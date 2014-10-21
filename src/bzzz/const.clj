(ns bzzz.const
  (:import (org.apache.lucene.util Version)))

(def ^{:dynamic true} *version* Version/LUCENE_CURRENT)
(def id-field "id")
(def default-root "/tmp/BZZZ")
(def default-port 3000)
(def default-identifier :__shard_0)
(def default-acceptable-discover-time-diff 10)



