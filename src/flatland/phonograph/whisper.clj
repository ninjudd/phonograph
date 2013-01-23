(ns flatland.phonograph.whisper
  (:require [gloss.core :refer [ordered-map enum repeated]]
            [gloss.io :refer [decode]])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel$MapMode]))

(def header-format
  (ordered-map
   :aggregate (enum :int32 :average :sum :last :max :min)
   :max-retention :int32
   :propagation-threshold :float32
   :archives (repeated (ordered-map 
                        :offset :int32
                        :density :int32
                        :count :int32))))

(defn map-file [file]
  (let [channel (.getChannel (RandomAccessFile. file "rw"))]
    (.map channel FileChannel$MapMode/READ_WRITE 0 (.size channel))))

(defn read-header [^ByteBuffer byte-buffer]
  (decode header-format (doto byte-buffer (.position 0)) false))
