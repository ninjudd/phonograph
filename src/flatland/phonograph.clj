(ns flatland.phonograph
  (:require [gloss.core :refer [compile-frame ordered-map enum repeated sizeof]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [decode]]
            [flatland.useful.map :refer [keyed update]])
  (:import [java.io File RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel$MapMode])
  (:use flatland.useful.debug))

(def header-format
  (ordered-map :aggregation (enum :int32 :average :sum :last :max :min)
               :max-retention :int32
               :propagation-threshold :float32))

(def archive-format
  (ordered-map :offset :int32
               :density :int32
               :count :int32))

(def point-format
  (compile-frame [:uint32 :float64]))

(def point-size
  (sizeof point-format))

(def aggregate-fn
  {:average #(double (/ (apply + %) (count %)))
   :sum (partial apply +)
   :min (partial apply min)
   :max (partial apply max)
   :last last})

(defn- trunc [density time]
  (- time (mod time density)))

(defn- offset [{:keys [density count]} base time]
  (* point-size
     (mod (/ (- time base) density) count)))

(defn- retention [{:keys [density count]}]
  (* density count))

(defn- slice-buffer [^ByteBuffer buffer position limit]
  (-> (.duplicate buffer)
      (.position position)
      (.limit limit)
      (.slice)))

(defn- read-points [buffer position limit]
  (let [limit (or limit (.capacity buffer))]
    (when (< position limit)
      (decode (repeated point-format :prefix :none)
              (slice-buffer buffer position limit)))))

(defn- write! [frame buffer offset value]
  (let [codec (compile-frame frame)]
    (.position buffer offset)
    (write-bytes codec buffer value)))

(defn- write-point! [buffer position point]
  (write! point-format buffer position point))

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

(defn- select-archive [interval archives]
  (first (filter (fn [{:keys [density count]}]
                   (<= interval (* density count)))
                 archives)))

(defn get-range [{:keys [max-retention archives now]} from until]
  (when (< until from)
    (throw (IllegalArgumentException.
            (format "Invalid time interval: from time '%s' is after until time '%s'" from until))))
  (let [now (or now (current-time))
        oldest (- now max-retention)]
    (if (or (< now from)
            (< until oldest))
      {}
      (let [from (max from oldest)
            until (min now until)
            archive (select-archive (- now from) archives)
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
            offset (offset archive base time)]
        (write-point! buffer offset [time value])))))

(defn append! [{:keys [aggregate archives]} & points]
  (let [archive (first archives)
        [from until] (apply (juxt min max) (map first points))]
    ;; validate points
    (when (< (retention archive) (- until from))
      (throw (IllegalArgumentException.
              (format "Cannot write multiple points that don't fit in the highest precision archive: %d-%d"
                      from until))))
    (write-points! archive points)
    ;; propagate to lower resolution archives
    (doseq [[higher lower] (partition 2 1 archives)]
      (let [from (trunc (:density lower) from)
            until (+ (trunc (:density lower) from)
                     (:density lower))]
        (write-points! archive
                       (->> (get-values higher from until)
                            (partition (/ (:density lower) (:density higher))) ; will divide evenly
                            (map aggregate)
                            (map vector (range from until (:density lower)))))))))

(defn- memmap-file [^RandomAccessFile file]
  (let [channel (.getChannel file)
        buffer (.map channel FileChannel$MapMode/READ_WRITE 0 (.size channel))
        close #(.close file)]
    (keyed [buffer close])))

(defn- archive-end [{:keys [offset count]}]
  (+ offset (* count point-size)))

(defn- add-offsets [archives header-size]
  (doall
   (rest
    (reductions (fn [prev archive]
                  (assoc archive
                    :offset (archive-end prev)))
                {:offset header-size :count 0}
                archives))))

(defn- validate-archives! [archives]
  (when (empty? archives)
    (throw (IllegalArgumentException. "Database must contain at least one archive")))
  (doseq [[a b] (partition 2 1 archives)]
    (when-not (< (:density a) (:density b))
      (throw (IllegalArgumentException.
              (format "Archives must be sorted from highest to lowest precision: %d >= %d"
                      (:density a) (:density b)))))
    (when-not (= 0 (mod (:density b) (:density a)))
      (throw (IllegalArgumentException.
              (format "Higher precision density must divide lower precision density evenly: %d/%d"
                      (:density b) (:density a)))))
    (let [points-needed (/ (:density b) (:density a))]
      (when (< (:count a) points-needed)
        (throw (IllegalArgumentException.
                (format "Higher precision archive must have enough points to propagate: %d < %d"
                        (:count a) points-needed)))))))

(defn- add-sliced-buffers [archives buffer]
  (doall
   (for [{:keys [offset count] :as archive} archives]
     (let [limit (+ offset (* point-size count))]
       (assoc archive
         :buffer (slice-buffer buffer offset limit))))))

(defn grow-file [^RandomAccessFile file size]
  ;; Should create a sparse file on Linux and OS X. May need additional logic for other systems.
  (.setLength file size))

(defn create [path opts & archives]
  (validate-archives! archives)
  (when (or (:overwrite opts) (.create (File. path)))
    (let [num-archives (count archives)
          header-size (+ (sizeof header-format)
                         (* num-archives (sizeof archive-format)))
          archives (add-offsets archives header-size)
          file (doto (RandomAccessFile. path "rw")
                 (grow-file (archive-end (last archives))))
          {:keys [buffer close]} (memmap-file file)
          header {:max-retention (retention (last archives))
                  :aggregation (:aggregation opts :sum)
                  :propagation-threshold (:propagation-threshold opts 0.0)}]
      (write! [header-format (repeated archive-format)] buffer 0 [header archives])
      (-> (merge header (keyed [path close archives]))
          (assoc :aggregate (aggregate-fn (:aggregation header)))
          (update :archives add-sliced-buffers buffer)))))

(defn open [path]
  (let [file (RandomAccessFile. path "rw")
        {:keys [buffer close]} (memmap-file file)
        [header archives] (decode [header-format (repeated archive-format)]
                                  buffer false)]
    (-> (merge header (keyed [path close archives]))
        (assoc :aggregate (aggregate-fn (:aggregation header)))
        (update :archives add-sliced-buffers buffer))))

(defn close [{:keys [close]}]
  (close))
