(defproject clojure-hadoop "1.0.0-SNAPSHOT"
  :description "writing map reduce jobs in clojure"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [commons-cli/commons-cli "1.2"]
                 [commons-httpclient/commons-httpclient "3.0.1"]
                 [commons-logging/commons-logging "1.1.1"]
                 [org.apache.mahout.hadoop/hadoop-core "0.20.1"]]
  :aot [
        clojure-hadoop.job
        clojure-hadoop.test.examples.wordcount1
        clojure-hadoop.test.examples.wordcount2
        clojure-hadoop.test.examples.wordcount3
        clojure-hadoop.test.examples.wordcount4
        clojure-hadoop.test.examples.wordcount5
        ]
  :dev-dependencies [[swank-clojure "1.2.1"]])
