(ns clojure-hadoop.wrap
  ;;#^{:doc "Map/Reduce wrappers that set up common input/output
  ;;conversions for Clojure jobs."}
  (:require [clojure-hadoop.imports :as imp]))

(imp/import-io)
(imp/import-mapreduce)

(declare *context*)

(defn string-map-reader
  "Returns a [key value] pair by calling str
   on the Writable key and value."
  [^Writable wkey ^Writable wvalue]
  [(str wkey) (str wvalue)])

(defn int-string-map-reader
  [^LongWritable wkey ^Writable wvalue]
  [(.get wkey) (str wvalue)])

(defn clojure-map-reader
  "Returns a [key value] pair by calling read-string on
   the string representations of the Writable key and value."
  [^Writable wkey ^Writable wvalue]
  [(read-string (str wkey)) (read-string (str wvalue))])

(defn clojure-reduce-reader
  "Returns a [key seq-of-values] pair by calling read-string
   on the string representations of the Writable key and values."
  [^Writable wkey wvalues]
  [(read-string (str wkey))
   (fn []
     (map (fn [^Writable v]
            (read-string (str v)))
          wvalues))])

(defn clojure-writer
  "Sends key and value to the context object by calling
   pr-str on key and value and wrapping them in Hadoop
   Text objects."
  [^TaskInputOutputContext context key value]
  (binding [*print-dup* true]
    (.write context
            (Text. (pr-str key))
            (Text. (pr-str value)))))

(defn wrap-map
  "Returns a function implementing the Mapper.map interface.

   f is a function of two arguments, key and value.
   f must return a *sequence* of *pairs* like
   [[key1 value1] [key2 value2] ...]

   When f is called, *context* is bound to the
   task's map context...
   (org.apache.hadoop.mapreduce.MapContext)

   reader is a function that receives the Writable key and
   value from Hadoop and returns a [key value] pair for f.

   writer is a function that receives each [key value] pair
   returned by f and writes the appropriately-type arguments
   to the Task's context...
   (org.apache.hadoop.mapreduce.TaskInputOutputContext)

   If not given, reader and writer default to
   clojure-map-reader and clojure-writer, respectively."
  ([f] (wrap-map f clojure-map-reader clojure-writer))
  ([f reader] (wrap-map f reader clojure-writer))
  ([f reader writer]
     (fn [this wkey wvalue context]
       (binding [*context* context]
         (doseq [pair (apply f (reader wkey wvalue))]
           (apply writer context pair))))))

(defn wrap-reduce
  "Returns a function implementing the Reducer.reduce interface.

   f is a function of two arguments. First argument is the key,
   second argument is a function, which takes no arguments and
   returns a lazy sequence of values. f must return a
   *sequence* of *pairs* like [[key1 value1] [key2 value2] ...]

   When f is called, *context* is bound to the
   task's reduce context...
   (org.apache.hadoop.mapreduce.ReduceContext)

   reader is a function that receives the Writable key and
   value from Hadoop and returns a [key value] pair for f.

   reader is a function that receives the Writable key and value
   from Hadoop and returns a [key values-function] pair for f.

   writer is a function that receives each [key value] pair
   returned by f and writes the appropriately-type arguments
   to the Task's context...
   (org.apache.hadoop.mapreduce.TaskInputOutputContext)

   If not given, reader and writer default to
   clojure-reduce-reader and  clojure-writer, respectively."
  ([f] (wrap-reduce f clojure-reduce-reader clojure-writer))
  ([f writer] (wrap-reduce f clojure-reduce-reader writer))
  ([f reader writer]
     (fn [this wkey wvalues context]
       (binding [*context* context]
         (doseq [pair (apply f (reader wkey wvalues))]
           (apply writer context pair))))))
