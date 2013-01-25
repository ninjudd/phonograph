(ns flatland.phonograph-test
  (:use clojure.test
        flatland.phonograph)
  (:require [flatland.useful.utils :refer [returning]]))

(defn temp-file
  "Create a temporary file, deleting the file when the JVM exits."
  ([] (temp-file "temp"))
  ([name] (temp-file name ".tmp"))
  ([name suffix]
     (doto (java.io.File/createTempFile name suffix)
       (.deleteOnExit))))

(defmacro with-temp-file
  "Execute body with file bound to a temporary file, deleting the file when done."
  [[file & args] & body]
  `(let [~file (temp-file ~@args)]
     (returning ~@body
       (.delete ~file))))

(deftest create-test
  (with-temp-file [foo]
    (let [phono (create foo {:overwrite true} {:count 100 :density 10})]
      (is (= nil (get-range (assoc phono :now 1000) 900 1000))))))
