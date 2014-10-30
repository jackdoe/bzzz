(ns bzzz.const
  (:import (org.apache.lucene.util Version)))

(def ^{:dynamic true :tag Version} *version* Version/LUCENE_CURRENT)
(def id-field "id")
(def default-root "/tmp/BZZZ")
(def default-port 3000)
(def default-size 20)
(def default-identifier :__shard_0)
(def default-acceptable-discover-time-diff 60)
