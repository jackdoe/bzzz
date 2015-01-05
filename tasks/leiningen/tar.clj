(ns leiningen.tar
  (:refer-clojure :exclude [replace])
  (:require leiningen.jar leiningen.clean
            [clojure.java.io :as io])
  (:use [leiningen.uberjar :only [uberjar]])
  (:use clojure.java.shell)
  (:use [clojure.string :only [join capitalize trim-newline replace]])
  (:import java.util.Date
           java.text.SimpleDateFormat))

(defn workarea
  [project]
  (io/file (:root project) "target" "tar"))

(defn cleanup
  [project]
  (sh "rm" "-rf" (str (workarea project))))

(defn reset
  [project]
  (cleanup project))

(defn get-version
  [project]
  (let [df (SimpleDateFormat. ".yyyyMMdd.HHmmss")]
    (replace (:version project) #"-SNAPSHOT" (.format df (Date.)))))

(defn make-tar [project]
  (let [build-version (get-version project)
        w (workarea project)
        jar-file (str (io/file (:root project)
                            "target"
                            (str "bzzz-"
                                 (:version project)
                                 "-standalone.jar")))
        tar-file (io/file (:root project) "binary" (str "bzzz-" build-version ".tar.gz"))
        usr-lib (str (io/file w "usr/lib/bzzz"))
        var-lib (str (io/file w "var/lib/bzzz"))
        etc (str (io/file w "etc/bzzz"))]
    (sh "mkdir" "-p"
        usr-lib
        etc
        var-lib)
    (sh "cp" jar-file (str (io/file usr-lib "bzzz.jar")))
    (sh "cp" (str (io/file (:root project) "pkg" "rpm" "start.sh")) usr-lib)
    (sh "cp" (str (io/file (:root project) "pkg" "bzzz-0.config")) etc)
    (sh "tar" "vczf" (str tar-file) "-C" (str w) ".")))

(defn tar
  ([project] (tar project true))
  ([project uberjar?]
     (reset project)
     (when uberjar? (uberjar project))
     (make-tar project)
     (cleanup project)))
