(ns clojure-hadoop.gen
  ^{:doc "Class-generation helpers for writing Hadoop jobs in Clojure."}
  (:import (org.apache.hadoop.util ToolRunner)))

(defmacro gen-job-classes
  "Creates gen-class forms for Hadoop job classes from the current
  namespace. Now you only need to write three functions:

  (defn mapper-map [this key value context] ...)
  (defn reducer-reduce [this key values context] ...)
  (defn tool-run [& args] ...)

  The first two functions are the standard map/reduce functions in any
  Hadoop job.

  The third function, tool-run, will be called by the Hadoop framework
  to start your job, with the arguments from the command line. It
  sets up the necessary configuration and waits for job completion,
  then return zero on success.

  You must also call gen-main-method to create the main method.

  After compiling your namespace, you can run it as a Hadoop job using
  the standard Hadoop command-line tools."
  []
  (let [the-name (.replace (str (ns-name *ns*)) \- \_)]
    `(do
       (gen-class
        :name ~the-name
        :extends "org.apache.hadoop.conf.Configured"
        :implements ["org.apache.hadoop.util.Tool"]
        :prefix "tool-"
        :main true)
       (gen-class
        :name ~(str the-name "_mapper")
        :extends "org.apache.hadoop.mapreduce.Mapper"
        :prefix "mapper-")
       (gen-class
        :name ~(str the-name "_reducer")
        :extends "org.apache.hadoop.mapreduce.Reducer"
        :prefix "reducer-"))))

(defn gen-main-method
  "Adds a standard main method, named tool-main, to the current
  namespace."
  []
  (let [the-name (.replace (str (ns-name *ns*)) \- \_)]
    (intern *ns*
            'tool-main
            (fn [& args]
              (System/exit
               (ToolRunner/run
                nil
                (.newInstance (Class/forName the-name))
                (into-array String args)))))))
