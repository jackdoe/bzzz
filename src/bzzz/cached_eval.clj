(ns bzzz.cached-eval
  (use bzzz.util)
  (:require [bzzz.index-stat :as index-stat])
  (:import (com.googlecode.concurrentlinkedhashmap ConcurrentLinkedHashMap ConcurrentLinkedHashMap$Builder)))

(defonce expr-cache ^java.util.Map
  (let [b ^java.util.Map (ConcurrentLinkedHashMap$Builder.)]
    (.maximumWeightedCapacity b 1000)
    (.build b)))

(defonce giant (Object.))

(defn get-or-eval [expr]
  (if-let [v (.get ^java.util.Map expr-cache expr)]
    v
    (locking giant
      (if-let [v (.get ^java.util.Map expr-cache expr)]
        v
        (let [t0 (time-ms)
              evaluated-expr (eval (read-string expr))]
          (index-stat/update-took-count index-stat/total
                                        "eval"
                                        (time-took t0))
          (.put ^java.util.Map expr-cache expr evaluated-expr)
          evaluated-expr)))))

