(ns flatland.phonograph
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
; pre-slice archive shizzle

    (keyed [path byte-buffer header])))

(def aggregate-fn
  {:average #(double (/ (apply + %) (count %)))
   :sum (partial apply +)
   :min (partial apply min)
   :max (partial apply max)
   :last last})

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

(defn- archive-buffer [{:keys [density count offset]} byte-buffer]
  (let [limit (+ offset (* (sizeof point-format) count))]
    (slice-byte-buffer byte-buffer offset limit)))

(defn- trunc [density time]
  (- time (mod time density)))

(defn- offset [{:keys [density count]} base time]
  (* (sizeof point-format)
     (rem (/ (- time base) density) count)))

(defn- retention [{:keys [density count]}]
  (* density count))

(defn- read-points [byte-buffer position limit]
  (decode (repeated point-format :prefix :none)
          (slice-byte-buffer byte-buffer position limit)))

(defn- write-point! [byte-buffer position point]
  (encode-to-buffer point-format
                    (slice-byte-buffer byte-buffer position nil)
                    [point]))

(defn- current-time []
  (quot (System/currentTimeMillis) 1000))

(defn- base-time [byte-buffer]
  (first (decode point-format byte-buffer false)))

(defn- get-values
  "Get a range of values from an archive between from and until. The byte-buffer passed in must
  be already sliced to only contain the bytes for this specific archive."
  [{:keys [density] :as archive} byte-buffer from until]
  (let [from (trunc density from)
        until (trunc density until)
        base (base-time byte-buffer)
        num (/ (- until from) density)]
    (if (zero? base)
      (repeat num nil)
      (let [from-offset (offset archive base from)
            until-offset (offset archive base until)]
        (map (fn [[time value]] ;; nil out points not within the time range
               (when (<= from time until)
                 value))
             (if (< from-offset until-offset)
               (read-points byte-buffer from-offset until-offset)
               (concat (read-points byte-buffer from-offset nil)
                       (read-points byte-buffer 0 until-offset))))))))

(defn get-range [{:keys [header byte-buffer now]} from until]
  (when (< until from)
    (throw (IllegalArgumentException.
            (format "Invalid time interval: from time '%s' is after until time '%s'" from until))))
  (let [now (or now (current-time))
        oldest (- now (:max-retention header))]
    (cond (< now from) {}
          (< until oldest) {}
          :else
          (let [from (max from oldest)
                until (min now until)
                archive (select-archive (- now from) (:archives header))
                buffer (archive-buffer archive byte-buffer)
                values (get-values archive buffer from until)
                density (:density archive)]
            (keyed [from until density values])))))

(defn- write-points!
  "Write points into the given archive. Points need to be already aggregated to match the resolution
  of the archive, as existing points in the archive are overwritten. The byte-buffer passed in must
  be already sliced to only contain the bytes for this specific archive."
  [{:keys [density] :as archive} byte-buffer points]
  (let [base (or (base-time byte-buffer)
                 (trunc density (ffirst points)))]
    (for [[time value] points]
      (let [time (trunc density time)
            offset (archive-offset archive base time)]
        (write-point! byte-buffer offset [time value])))))

(defn add-points! [{:keys [header byte-buffer now]} & points]
  (let [archive (first (:archives header))
        now (trunc (:density archive) (or now (current-time)))
        buffer (archive-buffer archive buffer)
        points (filter #(< (- now (retention archive)) % now) points)]
    (write-points! archive byte-buffer points)
    ;; propagate to lower resolution archives
    (doseq [[higher lower] (partition 2 1 archives)]
      (let [scale (/ (:density lower) (:density higher))
            oldest (- now (retention higher))
            values (get-values higher oldest now)
            buffer (archive-buffer archive buffer)]
        (->> (partition scale values)
             (map (aggregate-fn (:aggregate header)))
             (map vector (range oldest now (:density higher)))
             (write-points! archive buffer))))))
