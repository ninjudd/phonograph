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

(defn- select-archive [interval archives]
  (first (filter (fn [{:keys [density count]}]
                   (<= interval (* density count)))
                 archives)))

(defn- slice-byte-buffer [byte-buffer position limit]
  (-> byte-buffer
      (.duplicate)
      (.position position)
      (.limit (or limit (.capacity byte-buffer)))
      (.slice)))

(defn- archive-byte-buffer [{:keys [density count offset]} byte-buffer]
  (let [limit (+ offset (* (sizeof point-format) count))]
    (slice-byte-buffer byte-buffer offset limit)))

(defn- truncate [time density]
  (- time (mod time density)))

(defn- archive-offset [{:keys [density count]} base time]
  (* (sizeof point-format)
     (rem (/ (- time base) density) count)))

(defn- decode-points [position limit byte-buffer]
  (decode (repeated point-format :prefix :none)
          (slice-byte-buffer byte-buffer position limit)))

(defn- get-values [{:keys [offset density count] :as archive} byte-buffer from until]
  (let [byte-buffer (archive-byte-buffer archive byte-buffer)
        from (truncate from density)
        until (truncate until density)
        base (first (decode point-format byte-buffer false))
        num (/ (- until from) density)]
    (if (zero? base)
      (repeat num nil)
      (let [from-offset (archive-offset archive base from)
            until-offset (archive-offset archive base until)]
        (map (fn [[time value]] ;; remove points not within the range
               (when (<= from time until)
                 value))
             (if (< from-offset until-offset)
               (decode-points from-offset until-offset byte-buffer)
               (concat (decode-points from-offset nil byte-buffer)
                       (decode-points 0 until-offset byte-buffer))))))))

(defn get-range [{:keys [header byte-buffer now]} from until]
  (when (< until from)
    (throw (IllegalArgumentException.
            (format "Invalid time interval: from time '%s' is after until time '%s'" from until))))
  (let [now (or now (quot (System/currentTimeMillis) 1000))
        oldest (- now (:max-retention header))]
    (cond (< now from) {}
          (< until oldest) {}
          :else
          (let [from (max from oldest)
                until (min now until)
                archive (select-archive (- now from) (:archives header))
                density (:density archive)
                values (get-values archive byte-buffer from until)]
            (keyed [from until density values])))))
