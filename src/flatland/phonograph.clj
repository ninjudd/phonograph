(ns flatland.phonograph
  (:require [gloss.core :refer [compile-frame ordered-map enum repeated sizeof]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [decode]]
            [flatland.useful.map :refer [keyed update]]
            [flatland.useful.io :refer [mmap-file]])
  (:import [java.io File RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel$MapMode]))

(def header-format
  (ordered-map :aggregation (enum :int32 :average :sum :last :max :min)
               :max-retention :int32
               :propagation-threshold :float32))

(def archive-format
  (ordered-map :offset :int32
               :density :int32
               :count :int32))

(def point-format (compile-frame [:uint32 :float64]))
(def point-size (sizeof point-format))

(def aggregates
  {:average #(double (/ (apply + %) (count %)))
   :sum (partial apply +)
   :min (partial apply min)
   :max (partial apply max)
   :last last})

(defn aggregate-fn [kind]
  (if-let [aggregate (aggregates kind)]
    (fn [xs]
      (aggregate (remove nil? xs)))
    (throw (IllegalArgumentException. (str "Don't know how to aggregate by " kind)))))

(defn- floor [density time]
  (- time (mod time density)))

(defn- ceil [density time]
  (- time (mod time (- density))))

(defn- offset [archive base time]
  (* point-size
     (mod (/ (- time base)
             (:density archive))
          (:count archive))))

(defn- retention [archive]
  (when archive
    (* (:density archive)
       (:count archive))))

(defn- slice-buffer [^ByteBuffer buffer position limit]
  (let [^ByteBuffer b (-> (.duplicate buffer)
                          (.position position)
                          (.limit limit))]
    (.slice b)))

(defn- read-points [^ByteBuffer buffer position limit]
  (let [limit (or limit (.capacity buffer))]
    (when (< position limit)
      (decode (repeated point-format :prefix :none)
              (slice-buffer buffer position limit)))))

(defn- current-time []
  (quot (System/currentTimeMillis) 1000))

(defn- base-time [^ByteBuffer buffer]
  (let [buffer (.duplicate buffer)
        base (first (decode point-format buffer false))]
    (when-not (zero? base)
      base)))

(defn- verify-archive-range [archive from until]
  (let [retention (retention archive)]
    (doseq [bound [from until]]
      (when (neg? bound)
        (throw (IllegalArgumentException.
                (format "Negative value %d not permitted for range bound" bound)))))
    (when (< retention (- until from))
      (throw (IllegalArgumentException.
              (format "Range %d-%d does not fit into archive with retention %d"
                      from until retention))))))

(defn- get-values
  "Get a range of values from an archive between from and until. The buffer passed in must
  be already sliced to only contain the bytes for this specific archive."
  [{:keys [density buffer] :as archive} from until]
  (let [from (floor density from)
        until (floor density until)
        base (base-time buffer)
        num (/ (- until from) density)]
    (verify-archive-range archive from until)
    (if (nil? base)
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

(defn get-range
  "Fetch the range between from and until from the database. This will automatically read data from
  the highest precision archive that can provide all the data. Note that the current time is used in
  this calculation, so the archive used depends on how far back you are reading from now, not the
  size of your query range."
  [{:keys [max-retention archives now]} from until]
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
            archive (->> archives
                         (filter (comp (partial <= (- now from)) retention))
                         (first))
            values (get-values archive from until)
            density (:density archive)]
        (keyed [from until density values])))))

(defn get-all
  "Fetch all points in database at all precisions. Returns a sequence with each element matching the
  return format of get-range, one for each archive resolution."
  [{:keys [archives now]}]
  (let [until (or now (current-time))]
    (for [archive archives]
      (let [density (:density archive)
            from (max density (- until (retention archive)))
            values (get-values archive from until)]
        (keyed [from until density values])))))

(defn points [{:keys [from until density values]}]
  (apply sorted-map
         (interleave
          (range from until density)
          values)))

(defn get-all-points
  "Fetch a sequence of all points in the database, at the maximum resolution possible without
  losing any data. This will stitch together segments of data from different resolutions."
  [phono]
  (let [ranges (reverse (get-all phono))
        bounds (concat [(:from (first ranges))]
                       (for [[curr next] (partition 2 1 ranges)]
                         (ceil (:density curr) (:from next)))
                       [(:until (last ranges))])]
    (mapcat (fn [range [from until]]
              (subseq (points range) >= from < until))
            ranges
            (partition 2 1 bounds))))

(defn- write! [frame ^ByteBuffer buffer offset value]
  (let [codec (compile-frame frame)
        buffer (.duplicate buffer)]
    (.position buffer offset)
    (write-bytes codec buffer value)))

(defn- write-points!
  "Write points into the given archive. Points need to be already aggregated to match the resolution
  of the archive, as existing points in the archive are overwritten. The buffer passed in must
  be already sliced to only contain the bytes for this specific archive."
  [{:keys [density ^ByteBuffer buffer] :as archive} points]
  (let [base (or (base-time buffer)
                 (floor density (ffirst points)))]
    (doseq [[time value] points]
      (let [time (floor density time)
            offset (offset archive base time)]
        (when (zero? time)
          (throw (IllegalArgumentException.
                  (format "Cannot write point with time of 0"))))
        (write! point-format buffer offset [time value])))
    (.rewind buffer)))

(defn append!
  "Append the given points to the database. Note that you can only write a batch of points that
  fit within the highest precision archive, and you must write points in chronological order.
  Each point should be a [unix-time, value] pair."
  [{:keys [aggregate archives]} & points]
  {:pre [(every? #(= 2 (count %)) points)]}
  (let [archive (first archives)
        [from until] (apply (juxt min max)
                            (map first points))
        propagations (for [[higher lower] (partition 2 1 archives)]
                       (let [from (floor (:density lower) from)
                             until (ceil (:density lower) until)]
                         (keyed [higher lower from until])))]
    ;; Verify that the points can fit in the highest resolution archive.
    (verify-archive-range archive from until)
    ;; Verify that archives are aligned so we can propagate.
    (doseq [{:keys [higher from until]} propagations]
      (verify-archive-range higher from until))
    ;; Write to the highest resolution archive.
    (write-points! archive points)
    ;; Propagate to lower resolution archives.
    (doseq [{:keys [higher lower from until]} propagations]
      (write-points! lower
                     (->> (get-values higher from until)
                          (partition (/ (:density lower) (:density higher))) ; will divide evenly
                          (map aggregate)
                          (map vector (range from until (:density lower))))))))

(defn- archive-end [{:keys [offset count]}]
  (+ offset (* count point-size)))

(defn- add-offsets [archives header-size]
  (doall (rest (reductions (fn [prev archive]
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
  (doall (for [{:keys [offset count] :as archive} archives]
           (let [limit (+ offset (* point-size count))]
             (assoc archive
               :buffer (slice-buffer buffer offset limit))))))

(defn grow-file [^RandomAccessFile file size]
  ;; Should create a sparse file on Linux and OS X. May need additional logic for other systems.
  (.setLength file size))

(defn init-header [header buffer]
  (-> header
      (assoc :aggregate (aggregate-fn (:aggregation header)))
      (update :archives add-sliced-buffers buffer)))

(defn header-size [num-archives]
  (+ (sizeof header-format)
     (sizeof (compile-frame :int32)) ;; size of (repeated)'s header for num-archives
     (* num-archives (sizeof archive-format))))

(defn create
  "Create a new database file with the given configuration.
   Supported database options are:
    :aggregation           - name of aggregation method (see aggregates)
    :propagation-threshold - number of points that must have a value before propagating
    :overwrite             - whether to open an existing database file for writing
   Supported archive options are:
    :density - number of seconds per point; a lower number indicates higher precision
    :count   - total number of points in this archive"
  [^File path opts & archives]
  (validate-archives! archives)
  (when (or (.createNewFile path) (:overwrite opts))
    (let [num-archives (count archives)
          header-size (header-size num-archives)
          archives (add-offsets archives header-size)
          file (doto (RandomAccessFile. path "rw")
                 (grow-file (archive-end (last archives))))
          {:keys [buffer close]} (mmap-file file)
          header {:max-retention (retention (last archives))
                  :aggregation (:aggregation opts :sum)
                  :propagation-threshold (:propagation-threshold opts 0.0)}]
      (write! [header-format (repeated archive-format)] buffer 0 [header archives])
      (init-header (merge header (keyed [path close archives])) buffer))))

(defn open
  "Open an existing database file. This memory-maps the file and reads the header to determine which
  segments of the file contain each of the archives."
  [^File path]
  (let [file (RandomAccessFile. path "rw")
        {:keys [buffer close]} (mmap-file file)
        [header archives] (decode [header-format (repeated archive-format)]
                                  buffer false)]
    (init-header (merge header (keyed [path close archives])) buffer)))

(defn reopen
  "Reopen a database which has been closed. Will be faster than calling open on the database's path,
  because it reuses the header data rather than parsing it from the file again. As a consequence, if
  the underlying file has had its header modified since being closed, the behavior of this function
  is undefined.

  Note that this function does not cause the passed-in handle to become usable again; it returns a
  new handle which should be used to perform any reads or writes, and must be closed when no longer
  needed."
  [database]
  (let [^File path (:path database)
        {:keys [buffer close]} (mmap-file (RandomAccessFile. path "rw"))]
    (-> database
        (assoc :close close)
        (update :archives add-sliced-buffers buffer))))

(defn close
  "Close the provided database, and return a handle that can be used for reopening it. It is
  strongly recommended that, once you close a database, you do not hang onto its handle, because
  some pieces of it cannot be closed except via garbage collection (disgusting at this is - see
  http://docs.oracle.com/javase/6/docs/api/java/nio/MappedByteBuffer.html for reference). Retaining
  the handle returned by (close) is safe, of course, but if instead you retain the database passed
  to (close), you may leak OS resources."
  [database]
  (do
    ((:close database))
    (update database :archives
            (fn [archives]
              (doall (for [archive archives]
                       (dissoc archive :buffer)))))))
