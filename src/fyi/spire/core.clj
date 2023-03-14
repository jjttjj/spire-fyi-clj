(ns fyi.spire.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.data.csv :as csv]
            [clojure.core.async :as a]
            [datalevin.core :as dl]
            [tick.core :as t]
            [cognitect.aws.client.api :as aws]
            [babashka.fs :as fs]
            [taoensso.timbre :as log]
            [fyi.spire.flipside :as flipside]
            [fyi.spire.util :as u]
            [zprint.core :as zp])
  (:import [java.util.zip GZIPInputStream GZIPOutputStream]))

(defn get-col-names [csv-file]
  (with-open [r (-> csv-file fs/file io/reader)]
    (first (csv/read-csv r))))

(defn query-id->col-names [csv-root query-id]
  (get-col-names (first (fs/glob (fs/file csv-root query-id) "*.csv"))))

(defn init-config
  [{:keys [sql-dir csv-dir query-ids]}]
  (let [query-ids (or query-ids (->> csv-dir
                                     fs/list-dir
                                     (filter fs/directory?)
                                     (map fs/file-name)))
        queries   (mapv (fn [id]
                          (let [template-str (slurp (fs/file sql-dir (str id ".sql")))]
                            {:query/id              id
                             :query/type            :flipside
                             :query/schedule        {:every :day :at #time/time "09:00"}
                             :query/col-names       (query-id->col-names csv-dir id)
                             :query/template        template-str
                             :query.template/args   (u/template-args template-str)
                             :query.template/engine :jinja}))
                    query-ids)
        conf      {:queries queries}]
    (->> (zp/zprint conf {:output {:real-le? true :real-le-length 1}
                          :map    {:key-order [:query/id
                                               :query/type
                                               :query/col-names
                                               :query/schedule
                                               :query.template/engine
                                               :query.template/args
                                               :query/template]}})

         with-out-str
         (spit "config.edn"))))

(defn import-row-data [csv-root query-id]
  "Given a root directory containing directories of csv files named by query
  ids, return a iterable of datalevin kv transaction data to insert the row. Csv
  files will be loaded into memory one at a time."
  (let [csv-files (fs/glob (fs/file csv-root query-id) "*.csv")]
    (eduction
      (mapcat (fn [f]
                (with-open [r (-> f fs/file io/reader)]
                  (log/info "reading csv file:" f)
                  (vec (rest (csv/read-csv r))))))
      (map (fn [row]
             [:put query-id (first row) row]))
      csv-files)))

(defn import-query-rows! [{:keys [::kvdb]} csv-dir query-id]
  (dl/open-dbi kvdb query-id)
  (transduce
    (comp
      (partition-all 5000)
      ;;todo: log periodically, proporitonal to file size.
      #_(map-indexed (fn [ix x] (when (zero? (mod ix 500000)) (println "loading batch" ix)))))
    (completing
      (fn [_ batch]
        (dl/transact-kv kvdb batch)))
    nil
    (import-row-data csv-dir query-id)))

(defn import-all-rows! [{:keys [::kvdb] :as sys}
                        {:keys [csv-dir sql-dir query-ids]}]
  (let [query-ids (or query-ids (->> csv-dir
                                     fs/list-dir
                                     (filter fs/directory?)
                                     (map fs/file-name)))]
    (doseq [q query-ids]
      (log/info "importing data for query" q)
      (import-query-rows! sys csv-dir q))))

(defn qualify [n k] (keyword (name n) (name k)))

(defn render-sql [q-entity args]
  (let [template (:query/template q-entity)
        sql      (u/render-query template args)]
    sql))

(defn max-key-for [{:keys [::kvdb ::conn]} qid]
  (first (dl/get-first kvdb qid [:all-back])))

;;; defaults to t1/t2 from last key til now
(defn send-query
  ([sys query-id]
   (send-query sys query-id {:t1 (u/quote-sql-date (max-key-for sys query-id))
                             :t2 (u/quote-sql-date (u/fmt-sql-date (t/inst)))}))
  ([{:keys [::conn]} query-id args]
   (let [query    (dl/entity @conn [:query/id query-id])
         sql      (render-sql query args)
         response (flipside/send-query {:sql sql})
         response (update-keys response (partial qualify :flipside))
         job-id   (random-uuid)
         job
         (merge response
           {:job/id           job-id
            :job/query        [:query/id query-id]
            :job/status       ::submitted
            :job/submitted-at (t/inst)
            :args             args
            :sql              sql})]
     (dl/transact! conn [job])
     (dl/entity @conn [:job/id job-id]))))

(defn pending-jobs [{:keys [::conn ::kvdb]}]
  (let [db @conn]
    (->> (dl/q
           '[:find [?job ...]
             :in $
             :where
             [?job :job/status ?status]
             (or [?job :job/status ::submitted]
                 [?job :job/status :flipside/running])]
           db)
         (map #(dl/entity db %)))))

(defn csv-str [data]
  (with-open [out-str (java.io.StringWriter.)]
    (csv/write-csv out-str data)
    (.toString out-str)))

(defn save-csv [fname data & {:keys [gzip?]}]
  (with-open [out  (io/output-stream fname)
              out2 (if gzip? (GZIPOutputStream. out) out)
              w    (io/writer out2)]
    (csv/write-csv w data)))

(defn upload-csv [{:keys [::kvdb ::conn :aws/bucket]} query-id]
  (let [s3   (aws/client {:api :s3})
        body (->> (dl/get-range kvdb query-id [:all])
                  (map second)
                  csv-str
                  ;; todo: save to temp file or baos
                  .getBytes)]
    (log/info "uploading" query-id "to s3")
    (aws/invoke s3 {:op      :PutObject
                    :request {:Bucket bucket
                              :Key    (str "csv/" query-id ".csv")
                              :Body   body}})))

(defn update-job! [{:keys [::conn ::kvdb :aws/bucket] :as sys} job]
  (let [query-id (-> job :job/query :query/id)
        result   (-> (flipside/check-result {:token (:flipside/token job)})
                     (update-keys (partial qualify :flipside))
                     (assoc :db/id (:db/id job)))
        fstatus  (:flipside/status result)
        [with-status new-rows]
        (cond
          (= fstatus "running")
          [{:job/status :flipside/running} nil]

          (= fstatus "finished")
          [{:job/status      :flipside/finished
            :job/finished-at (t/inst)}
           (mapv
             (fn [row] [:put query-id (first row) (mapv str row)])
             (:flipside/results result))]

          (:flipside/errors result)
          (do
            (log/warn "query resulted in error" result)
            [{:job/status :flipside/error}
             nil])

          :else
          (throw (ex-info "Unknown status" result)))
        new-job (merge result with-status)]
    (dl/transact! conn [new-job])
    (when new-rows
      (dl/transact-kv kvdb new-rows)
      (upload-csv sys query-id))
    (dl/entity @conn [:job/id (:job/id job)])))

(defn check-pending! [{:keys [::conn ::kvdb] :as sys}]
  (mapv
    (fn [job]
      (update-job! sys job))
    (pending-jobs sys)))

;;; Todo: make smarter
(defn wait-for-results [sys]
  (loop [n 0]
    (if (empty? (check-pending! sys))
      (log/info "All query results received on attempt" n)
      (if (= n 30)
        (log/warn "Timeout waiting for results")
        (do (Thread/sleep 60000)
            (recur (inc n)))))))

(defn schedule->next [{:keys [every at in]} now]
  (assert (= every :day) "TODO: non `{:every :day}` schedules")
  (let [td (-> (t/today) (t/at at))]
    (-> td
        (cond-> (t/<= now td) (t/>> #time/period "p1d"))
        (t/in (or in "UTC"))
        t/instant)))

(defn schedule-queries [sys]
  (let [queries (dl/q '[:find [(pull ?q [*]) ...] :where [?q :query/id]] @(::conn sys))]
    (doseq [[sched qs] (group-by :query/schedule queries)]
      (a/thread
        (try
          (loop [now (t/now)]
            (let [nxt    (schedule->next sched now)
                  millis (t/millis (t/between (schedule->next sched now) now))]
              (a/<!! (a/timeout millis))
              (doseq [q qs] (send-query sys q))
              (a/thread (wait-for-results sys))
              ;; do we need to account for time drift here?
              (recur (t/now))))
          (catch Throwable t
            (log/error t "Error in scheduled query")))))))

(def schema {:query/id  {:db/valueType :db.type/string
                         :db/unique    :db.unique/identity}
             :job/id    {:db/valueType :db.type/uuid
                         :db/unique    :db.unique/identity}
             :job/query {:db/type :db.type/ref}})

(defn start
  ([]
   (start (edn/read-string {:readers *data-readers*} (slurp "config.edn"))))
  ([{:keys [data-path queries] :as conf}]
   (let [kvdb (dl/open-kv  (str (fs/file data-path "datalevin" "kvdb-store")))
         conn (dl/get-conn (str (fs/file data-path "datalevin" "datalog-store")) schema)
         sys  (merge (select-keys conf [:aws/bucket :aws/region])
                {::kvdb kvdb
                 ::conn conn})]
     (dl/update-schema conn schema)
     (dl/transact! conn queries)
     (doseq [q queries]
       (dl/open-dbi kvdb (:query/id q)))
     (schedule-queries sys)
     sys)))

(defn stop [{::keys [kvdb conn]}]
  (dl/close-kv kvdb)
  (dl/close conn)
  nil)


(defonce sys nil)
(defn start! [_] (alter-var-root #'sys (fn [s] (if s s (do (println "starting") (start))))))
(defn stop! [_] (alter-var-root #'sys (fn [s] (when s (println "stopping") (stop s)))))
