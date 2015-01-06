(ns leiningen.fatrpm
  (:refer-clojure :exclude [replace])
  (:use [clojure.java.shell :only [sh]]
        [clojure.java.io :only [file delete-file writer copy as-file]]
        [clojure.string :only [join capitalize trim-newline replace]]
        [leiningen.uberjar :only [uberjar]])
  (:import java.util.Date
           java.text.SimpleDateFormat
           (org.codehaus.mojo.rpm RPMMojo
                                  AbstractRPMMojo
                                  Mapping Source
                                  SoftlinkSource
                                  Scriptlet)
           (org.apache.maven.project MavenProject)
           (org.apache.maven.shared.filtering DefaultMavenFileFilter)
           (org.codehaus.plexus.logging.console ConsoleLogger)))

(defn write
  "Write string to file, plus newline"
  [file string]
  (with-open [w (writer file)]
    (.write w (str (trim-newline string) "\n"))))

(defn workarea
  [project]
  (file (:root project) "target" "rpm"))

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

(defn set-mojo!
  [object name value]
  (let [field (.getDeclaredField AbstractRPMMojo name)]
    (.setAccessible field true)
    (.set field object value))
  object)

(defn array-list
  [list]
  (let [list (java.util.ArrayList.)]
    (doseq [item list] (.add list item))
    list))

(defn scriptlet
  "Creates a scriptlet backed by a file"
  [filename]
  (doto (Scriptlet.)
    (.setScriptFile (file filename))))

(defn source
  ([] (Source.))
  ([location]
     (doto (Source.)
       (.setLocation (str location))))
  ([location destination]
     (doto (Source.)
       (.setLocation (str location))
       (.setDestination (str destination)))))

(defn mapping
  [m]
  (doto (Mapping.)
    (.setArtifact           (:artifact m))
    (.setConfiguration      (case (:configuration m)
                              true  "true"
                              false "false"
                              nil   "false"
                              (:configuration m)))
    (.setDependency         (:dependency m))
    (.setDirectory          (:directory m))
    (.setDirectoryIncluded  (boolean (:directory-included? m)))
    (.setDocumentation      (boolean (:documentation? m)))
    (.setFilemode           (:filemode m))
    (.setGroupname          (:groupname m))
    (.setRecurseDirectories (boolean (:recurse-directories? m)))
    (.setSources            (:sources m))
    (.setUsername           (:username m))))

(defn mappings
  [project]
  (map (comp mapping
             (partial merge {:username "bzzz"
                             :groupname "bzzz"}))
       [; Jar
        {:directory "/usr/lib/bzzz/"
         :filemode "755"
         :sources [(source (file (:root project) "pkg" "rpm" "start.sh")
                           "start.sh")]}

        {:directory "/usr/lib/bzzz/"
         :filemode "644"
         :sources [(source (str (file (:root project)
                                      "target"
                                      (str "bzzz-"
                                           (:version project)
                                           "-standalone.jar")))
                           "bzzz.jar")]}


        {:directory "/var/lib/bzzz/"
         :filemode "755"
         :username "bzzz"
         :groupname "bzzz"
         :directory-included? true}

        {:directory "/etc/bzzz"
         :filemode "755"
         :directory-included? true}

        {:directory "/etc/bzzz"
         :filemode "644"
         :configuration "noreplace"
         :sources [(source (file (:root project) "pkg" "bzzz-0.config")
                           "bzzz-0.config")]}

        {:directory "/etc/init.d"
         :filemode "755"
         :username "root"
         :groupname "root"
         :sources [(source (file (:root project) "pkg" "rpm" "init.sh")
                           "bzzz")]}]))

(defn create-dependency
  [rs]
  (let [hs (java.util.LinkedHashSet.)]
    (doseq [r rs] (.add hs r))
    hs))

(defn make-rpm
  [project]
  (let [mojo (RPMMojo.)
        fileFilter (DefaultMavenFileFilter.)]
    (.enableLogging fileFilter (ConsoleLogger. 0 "Logger"))
    (set-mojo! mojo "project" (MavenProject.))
    (set-mojo! mojo "mavenFileFilter" fileFilter)
    (set-mojo! mojo "projversion" (get-version project))
    (set-mojo! mojo "name" (:name project))
    (set-mojo! mojo "summary" (:description project))
    (set-mojo! mojo "group" "bzzz")
    (set-mojo! mojo "description" (:description project))
    (set-mojo! mojo "copyright" "Borislav Nikolov")
    (set-mojo! mojo "workarea" (workarea project))
    (set-mojo! mojo "mappings" (mappings project))
    (set-mojo! mojo "preinstallScriptlet" (scriptlet
                                           (file (:root project)
                                                 "pkg" "rpm" "preinst.sh")))
    (set-mojo! mojo "requires" (create-dependency ["java >= 1.7.0"]))
    (set-mojo! mojo "defineStatements" ["_source_filedigest_algorithm 1",
                                        "_binary_filedigest_algorithm 1",
                                        "_source_payload w9.gzdio",
                                        "_binary_payload w9.gzdio"])
    (.execute mojo)))

(defn extract-rpm
  [project]
  (let [
        top (file (workarea project)
                  (:name project)
                  "RPMS")
        dir (file top "noarch")
        rpms (remove #(.isDirectory %) (.listFiles dir))]
    (doseq [rpm rpms]
      (let [dest (file (:root project) "binary" (.getName rpm))]
        (.renameTo rpm dest)
        (write (str dest ".md5")
               (if (.exists (as-file "/sbin/md5"))
                 (:out (sh "md5" (str dest)))
                 (:out (sh "md5sum" (str dest)))))))))

(defn fatrpm
  ([project] (fatrpm project true))
  ([project uberjar?]
     (reset project)
     (when uberjar? (uberjar project))
     (make-rpm project)
     (extract-rpm project)
     (cleanup project)))
