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
   :archives (repeated (ordered-map :offset :int32
                                    :density :int32
                                    :count :int32))))

(def point-format
  (compile-frame [:uint32 :float64]))

(defn- slice-buffer [^ByteBuffer buffer position limit]
  (-> (.duplicate buffer)
      (.position position)
      (.limit (or limit (.capacity buffer)))
      (.slice)))

(defn- slice-buffers [archives buffer]
  (for [{:keys [offset count] :as archive} archives]
    (let [limit (+ offset (* (sizeof point-format) count))]
      (assoc archive
        :buffer (slice-buffer buffer offset limit)))))

(defn open-file [path]
  (let [channel (.getChannel (RandomAccessFile. path "rw"))
        buffer (.map channel FileChannel$MapMode/READ_WRITE 0 (.size channel))
        header (-> (decode header-format buffer false)
                   (update :archives slice-buffers buffer))]
    (keyed [path buffer header])))

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

(defn- trunc [density time]
  (- time (mod time density)))

(defn- offset [{:keys [density count]} base time]
  (* (sizeof point-format)
     (rem (/ (- time base) density) count)))

(defn- retention [{:keys [density count]}]
  (* density count))

(defn- read-points [buffer position limit]
  (decode (repeated point-format :prefix :none)
          (slice-buffer buffer position limit)))

(defn- write-point! [buffer position point]
  (encode-to-buffer point-format
                    (slice-buffer buffer position nil)
                    [point]))

(defn- current-time []
  (quot (System/currentTimeMillis) 1000))

(defn- base-time [buffer]
  (first (decode point-format buffer false)))

(defn- get-values
  "Get a range of values from an archive between from and until. The buffer passed in must
  be already sliced to only contain the bytes for this specific archive."
  [{:keys [density buffer] :as archive} from until]
  (let [from (trunc density from)
        until (trunc density until)
        base (base-time buffer)
        num (/ (- until from) density)]
    (if (zero? base)
      (repeat num nil)
      (let [from-offset (offset archive base from)
            until-offset (offset archive base until)]
        (map (fn [[time value]] ;; nil out points not within the time range
               (when (<= from time until)
                 value))
             (if (< from-offset until-offset)
               (read-points buffer from-offset until-offset)
               (concat (read-points buffer from-offset nil)
                       (read-points buffer 0 until-offset))))))))

(defn get-range [{:keys [header buffer now]} from until]
  (when (< until from)
    (throw (IllegalArgumentException.
            (format "Invalid time interval: from time '%s' is after until time '%s'" from until))))
  (let [now (or now (current-time))
        oldest (- now (:max-retention header))]
    (if (or (< now from)
            (< until oldest))
      {}
      (let [from (max from oldest)
            until (min now until)
            archive (select-archive (- now from) (:archives header))
            values (get-values archive from until)
            density (:density archive)]
        (keyed [from until density values])))))

(defn- write-points!
  "Write points into the given archive. Points need to be already aggregated to match the resolution
  of the archive, as existing points in the archive are overwritten. The buffer passed in must
  be already sliced to only contain the bytes for this specific archive."
  [{:keys [density buffer] :as archive} points]
  (let [base (or (base-time buffer)
                 (trunc density (ffirst points)))]
    (for [[time value] points]
      (let [time (trunc density time)
            offset (archive-offset archive base time)]
        (write-point! buffer offset [time value])))))

(defn add-points! [{:keys [header buffer now]} & points]
  (let [archive (first (:archives header))
        now (trunc (:density archive) (or now (current-time)))
        points (filter #(< (- now (retention archive)) % now) points)]
    (write-points! archive points)
    ;; propagate to lower resolution archives
    (doseq [[higher lower] (partition 2 1 archives)]
      (let [oldest (- now (retention higher))]
        (write-points!
         (->> (get-values higher oldest now)
              (partition (/ (:density lower) (:density higher))) ; will divide evenly
              (map (aggregate-fn (:aggregate header)))
              (map vector (range oldest now (:density higher)))))))))
