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

(deftest create-empty-file
  (with-temp-file [f]
    (let [phono (create f {:overwrite true} {:count 100 :density 10} {:count 100 :density 100})
          range (get-range (assoc phono :now 1000) 800 1000)]
      (is (= 800  (:from range)))
      (is (= 1000 (:until range)))
      (is (= 10   (:density range)))
      (is (= (repeat 20 nil) (:values range)))
      (let [range (get-range (assoc phono :now 10000) 8000 10000)]
        (is (= 8000  (:from range)))
        (is (= 10000 (:until range)))
        (is (= 100   (:density range)))
        (is (= (repeat 20 nil) (:values range)))))))

(deftest create-and-reopen
  (with-temp-file [f]
    (let [phono (create f {:overwrite true} {:count 100 :density 10} {:count 100 :density 100})]
      (close phono)
      (let [phono (open f)]
        (is (= :sum  (:aggregation phono)))
        (is (= 10000 (:max-retention phono)))
        (is (= 0.0   (:propagation-threshold phono)))
        (is (= [100 100] (map :count (:archives phono))))
        (is (= [10 100]  (map :density (:archives phono))))
        (is (= [36 1236] (map :offset (:archives phono))))))))

(deftest append-data
  (with-temp-file [f]
    (let [phono (create f {:overwrite true} {:count 100 :density 10} {:count 100 :density 100})
          points (map vector (range 10 1010 10) (range 100))]
      (apply append! (assoc phono :now 1000) points)
      (let [range (get-range (assoc phono :now 1000) 800 1000)]
        (is (= 800  (:from range)))
        (is (= 1000 (:until range)))
        (is (= 10   (:density range)))
        (is (= (map last points) (:values range)))))))