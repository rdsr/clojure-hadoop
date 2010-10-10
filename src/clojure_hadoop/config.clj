(ns clojure-hadoop.config
  (:require [clojure-hadoop.imports :as imp]
            [clojure-hadoop.load :as load])
  (:import (org.apache.hadoop.io.compress
            DefaultCodec GzipCodec)))

;; This file defines configuration options for clojure-hadoop.
;;
;; The SAME options may be given either on the command line (to
;; clojure_hadoop.job) or in a call to defjob.
;;
;; In defjob, option names are keywords.  Values are symbols or
;; keywords.  Symbols are resolved as functions or classes.  Keywords
;; are converted to Strings.
;;
;; On the command line, option names are preceeded by "-".
;;
;; Options are defined as methods of the configure multimethod.
;; Documentation for individual options appears with each method,
;; below.

(imp/import-io)
(imp/import-fs)
(imp/import-mapreduce)
(imp/import-mapreduce-lib)

(defn- ^String as-str [s]
  (cond (keyword? s)
        (name s)
        (class? s)
        (.getName ^Class s)
        (fn? s)
        (throw (Exception. "Cannot use function as value; use a symbol."))
        :else (str s)))

(defn- set-conf
  [^Job job key value]
  (.. job getConfiguration (set key (as-str value))))

(defmulti configure (fn [job key value] key))

(defmethod configure :job
  [job key value]
  (let [f (load/load-name value)]
    (doseq [[k v] (f)]
      (configure job k v))))

;; Job input paths, separated by commas, as a String.
(defmethod configure :input
  [^Job job key value]
  (FileInputFormat/setInputPaths job (as-str value)))

;; Job output path, as a String.
(defmethod configure :output
  [^Job job key value]
  (FileOutputFormat/setOutputPath job (Path. (as-str value))))

;; When true or "true", deletes output path before starting.
(defmethod configure :replace
  [^Job job key value]
  (when (= (as-str value) "true")
    (set-conf
     job
     "clojure-hadoop.job.replace"
     "true")))

;; The mapper function.  May be a class name or a Clojure function as
;; namespace/symbol.  May also be "identity" for IdentityMapper.
(defmethod configure :map
  [^Job job key value]
  (let [value (as-str value)]
    (cond
     ;; todo: later
     ;; (= "identity" value)
     ;; (.setClass conf "mapreduce.map.class" IdentityMapper Mapper/class)
     (.contains value "/")
     (set-conf job "clojure-hadoop.job.map" value)
     :else
     (.setMapperClass job (Class/forName value)))))

;; The reducer function.  May be a class name or a Clojure function as
;; namespace/symbol.  May also be "identity" for IdentityReducer or
;; "none" for no reduce stage.
(defmethod configure :reduce
  [^Job job key value]
  (let [value (as-str value)]
    (cond
     ;; todo: later
     ;; (= "identity" value)
     ;; (.setReducerClass conf IdentityReducer)
     (= "none" value)
     (.setNumReduceTasks job 0)
     (.contains value "/")
     (set-conf job "clojure-hadoop.job.reduce" value)
     :else
     (.setReducerClass job (Class/forName value)))))

;; The mapper reader function, converts Hadoop Writable types to
;; native Clojure types.
(defmethod configure :map-reader
  [^Job job key value]
  (set-conf job "clojure-hadoop.job.map.reader" value))

;; The mapper writer function; converts native Clojure types to Hadoop
;; Writable types.
(defmethod configure :map-writer
  [^Job job key value]
  (set-conf job "clojure-hadoop.job.map.writer" value))

;; The mapper output key class; used when the mapper writer outputs
;; types different from the job output.
(defmethod configure :map-output-key
  [^Job job key value]
  (.setMapOutputKeyClass job (Class/forName value)))

;; The mapper output value class; used when the mapper writer outputs
;; types different from the job output.
(defmethod configure :map-output-value
  [^Job job key value]
  (.setMapOutputValueClass job (Class/forName value)))

;; The job output key class.
(defmethod configure :output-key
  [^Job job key value]
  (.setOutputKeyClass job (Class/forName value)))

;; The job output value class.
(defmethod configure :output-value
  [^Job job key value]
  (.setOutputValueClass job (Class/forName value)))

;; The reducer reader function, converts Hadoop Writable types to
;; native Clojure types.
(defmethod configure :reduce-reader
  [^Job job key value]
  (set-conf job
            "clojure-hadoop.job.reduce.reader"
            (as-str value)))

;; The reducer writer function; converts native Clojure types to
;; Hadoop Writable types.
(defmethod configure :reduce-writer
  [^Job job key value]
  (set-conf job
            "clojure-hadoop.job.reduce.writer"
            (as-str value)))

;; The input file format.
;; May be a class name or
;; "text" for TextInputFormat,
;; "seq" for SequenceFileInputFormat.
(defmethod configure :input-format
  [^Job job key value]
  (cond
   (= "text" value)
   (.setInputFormatClass job TextInputFormat)
   ;; todo: (= "kvtext" value)
   (= "seq" value)
   (.setInputFormatClass job SequenceFileInputFormat)
   :else
   (.setInputFormatClass job (Class/forName value))))

;; The output file format.  May be a class name or "text" for
;; TextOutputFormat, "seq" for SequenceFileOutputFormat.
(defmethod configure :output-format
  [^Job job key value]
  (let [value (as-str value)]
    (cond
     (= "text" value)
     (.setOutputFormatClass job TextOutputFormat)
     (= "seq" value)
     (.setOutputFormatClass job SequenceFileOutputFormat)
     :else
     (.setOutputFormatClass job (Class/forName value)))))

;; If true, compress job output files.
(defmethod configure :compress-output
  [^Job job key value]
  (cond
   (= "true" (as-str value))
   (FileOutputFormat/setCompressOutput job true)
   (= "false" (as-str value))
   (FileOutputFormat/setCompressOutput job false)
   :else (throw
          (Exception.
           "compress-output value must be true or false"))))

;; Codec to use for compressing job output files.
(defmethod configure :output-compressor
  [^Job job key value]
  (cond
   (= "default" (as-str value))
   (FileOutputFormat/setOutputCompressorClass
    job DefaultCodec)
   (= "gzip" (as-str value))
   (FileOutputFormat/setOutputCompressorClass
    job GzipCodec)
   ;; todo: later
   ;; (= "lzo" (as-str value))
   ;; (FileOutputFormat/setOutputCompressorClass
   ;;  conf LzoCodec)
   :else
   (FileOutputFormat/setOutputCompressorClass
    job (Class/forName value))))

;; Type of compression to use for sequence files.
(defmethod configure :compression-type
  [^Job job key value]
  (cond
   (= "block" (as-str value))
   (SequenceFileOutputFormat/setOutputCompressionType
    job SequenceFile$CompressionType/BLOCK)

   (= "none" (as-str value))
   (SequenceFileOutputFormat/setOutputCompressionType
    job SequenceFile$CompressionType/NONE)

   (= "record" (as-str value))
   (SequenceFileOutputFormat/setOutputCompressionType
    job SequenceFile$CompressionType/RECORD)))

(defn parse-command-line-args [^Job job args]
  (when (empty? args)
    (throw (Exception. "Missing required options.")))
  (when-not (even? (count args))
    (throw (Exception. "Number of options must be even.")))
  (doseq [[k v] (partition 2 args)]
    (configure job (keyword (subs k 1)) v)))

(defn print-usage []
  (println "Usage: java -cp [jars...] clojure_hadoop.job [options...]
Required options are:
 -input     comma-separated input paths
 -output    output path
 -map       mapper function, as namespace/name or class name
 -reduce    reducer function, as namespace/name or class name
OR
 -job       job definition function, as namespace/name

Mapper or reducer function may also be \"identity\".
Reducer function may also be \"none\".

Other available options are:
 -input-format      Class name or \"text\" or \"seq\" (SeqFile)
 -output-format     Class name or \"text\" or \"seq\" (SeqFile)
 -output-key        Class for job output key
 -output-value      Class for job output value
 -map-output-key    Class for intermediate Mapper output key
 -map-output-value  Class for intermediate Mapper output value
 -map-reader        Mapper reader function, as namespace/name
 -map-writer        Mapper writer function, as namespace/name
 -reduce-reader     Reducer reader function, as namespace/name
 -reduce-writer     Reducer writer function, as namespace/name
 -name              Job name
 -replace           If \"true\", deletes output dir before start
 -compress-output   If \"true\", compress job output files
 -output-compressor Compression class or \"gzip\",\"lzo\",\"default\"
 -compression-type  For seqfiles, compress \"block\",\"record\",\"none\"
"))
