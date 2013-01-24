(ns flatland.phonograph.whisper
  (:require [gloss.core :refer [compile-frame ordered-map enum repeated sizeof]]
            [gloss.io :refer [decode]]
            [flatland.useful.map :refer [keyed]])
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

(def point-format
  (compile-frame
   [:uint32 :float64]))

(defn open-file [path]
  (let [channel     (.getChannel (RandomAccessFile. path "rw"))
        byte-buffer (.map channel FileChannel$MapMode/READ_WRITE 0 (.size channel))
        header      (decode header-format byte-buffer false)]
    (keyed [path byte-buffer header])))

(defn archive-include? [interval {:keys [density count]}]
  (<= interval (* density count)))

(defn- truncate [time density]
  (- time (mod time density)))

(defn- slot [{:keys [density count]} base time]
  (mod (/ (- time base) density) count))

(defn- decode-points [offset slot num byte-buffer]
  (.position byte-buffer (+ offset (* slot (sizeof point-format))))
  (decode (compile-frame (repeat num point-format)) byte-buffer false))

(defn- get-values [{:keys [offset density count] :as archive} byte-buffer from until]
  (.position byte-buffer offset)
  (let [from (truncate from density)
        until (truncate until density)
        base (first (decode point-format byte-buffer false))
        num (/ (- until from) density)]
    (if (zero? base)
      (repeat num nil)
      (let [from-slot (slot archive base from)
            until-slot (slot archive base until)]
        (map (fn [[time value]]
               (prn [time value])
               (when (<= from time until)
                 value))
             (if (< from-slot until-slot)
               (decode-points offset from-slot num byte-buffer)
               (let [from-count (- count from-slot)
                     until-count (- num from-count)]
                 (concat (decode-points offset from-slot from-count byte-buffer)
                         (decode-points offset 0 until-count byte-buffer)))))))))

(defn get-range [{:keys [header byte-buffer now]} from until]
  (when (< until from)
    (throw (IllegalArgumentException.
            (format "Invalid time interval: from time '%s' is after until time '%s'" from until))))
  (let [now (or now (quot (System/currentTimeMillis) 1000))
        oldest (- now (:max-retention header))]
    (prn now oldest)
    (cond (< now from) {}
          (< until oldest) {}
          :else
          (let [from (max from oldest)
                until (min now until)
                archive (first (filter (partial archive-include? (- now from))
                                       (:archives header)))
                density (:density archive)
                values (get-values archive byte-buffer from until)]
            (keyed [from until density values])))))
