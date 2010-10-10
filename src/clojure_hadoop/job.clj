(ns clojure-hadoop.job
  (:use [clojure.contrib.def :only (defvar-)])
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.config :as config]
            [clojure-hadoop.load :as load])
  (:import (org.apache.hadoop.util Tool)))

(imp/import-io)
(imp/import-io-compress)
(imp/import-fs)
(imp/import-mapreduce)
(imp/import-mapreduce-lib)

(gen/gen-job-classes)
(gen/gen-main-method)

(defvar- method-fn-name
     {"map" "mapper-map" "reduce" "reducer-reduce"})

(defvar- wrapper-fn
     {"map" wrap/wrap-map "reduce" wrap/wrap-reduce})

(defvar- default-reader
     {"map" wrap/clojure-map-reader "reduce" wrap/clojure-reduce-reader})

(defn- setup-functions
  "Preps the mapper or reducer with a Clojure function read
   from the job configuration. Called from Mapper.setup and
   Reducer.setup"
  [type context]
  (letfn [(read-conf
           [key]
           (-> context
               .getConfiguration
               (.get key)))]
    (let [function (load/load-name (read-conf (str "clojure-hadoop.job." type)))
          reader (if-let [v (read-conf (str "clojure-hadoop.job." type ".reader"))]
                   (load/load-name v)
                   (default-reader type))
          writer (if-let [v (read-conf (str "clojure-hadoop.job." type ".writer"))]
                 (load/load-name v)
                 wrap/clojure-writer)]
      (assert (fn? function))
      (alter-var-root
       (ns-resolve (the-ns 'clojure-hadoop.job)
                   (symbol (method-fn-name type)))
       (fn [_]
         ((wrapper-fn type) function reader writer))))))

;;; CREATING AND CONFIGURING JOBS
(defn- parse-command-line [job args]
  (try
    (config/parse-command-line-args job args)
    job
    (catch Exception e
      (prn e)
     (config/print-usage)
     (System/exit 1))))

(defn- handle-replace-option [^Job job]
  (let [conf (.getConfiguration job)]
    (when (= "true" (.get conf "clojure-hadoop.job.replace"))
      (let [fs (FileSystem/get conf)
            output (FileOutputFormat/getOutputPath job)]
        (.delete fs output true)))))

(defn- set-default-config [^Job job]
  (doto (Job.)
    (.setJobName "clojure_hadoop.job")
    (.setOutputKeyClass Text)
    (.setOutputValueClass Text)
    (.setMapperClass (Class/forName "clojure_hadoop.job_mapper"))
    (.setReducerClass (Class/forName "clojure_hadoop.job_reducer"))
    (.setInputFormatClass SequenceFileInputFormat)
    (.setOutputFormatClass SequenceFileOutputFormat)
    (FileOutputFormat/setCompressOutput true)
    (SequenceFileOutputFormat/setOutputCompressionType
     SequenceFile$CompressionType/BLOCK)))

(defn run
  "Runs a Hadoop job given the Job object."
  [job]
  (doto job
    (handle-replace-option)
    (.waitForCompletion true)))

;;; MAPPER METHODS
(defn mapper-setup [this context]
  (setup-functions "map" context))

(defn mapper-map [_ _ _ _]
  (throw (Exception. "Mapper function not defined.")))

;;; REDUCER METHODS
(defn reducer-setup [this context]
  (setup-functions "reduce" context))

(defn reducer-reduce [_ _ _ _]
  (throw (Exception. "Reducer function not defined.")))

;;; TOOL METHODS
(defn tool-run [^Tool this args]
  (letfn [(response
           [v] (if (true? v) 0 1))]
    (-> (.getConf this)
        Job.
        set-default-config
        (parse-command-line args)
        run
        response)))
