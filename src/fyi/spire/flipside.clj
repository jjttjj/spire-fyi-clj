(ns fyi.spire.flipside
  (:require [babashka.http-client :as http]
            [babashka.json :as json]
            [tick.core :as t]
            [clojure.string :as str]))

(defn- get-api-key []
  (or (System/getenv "FLIPSIDE_API_KEY")
      (System/getProperty "FLIPSIDE_API_KEY")
      (throw (ex-info "FLIPSIDE_API_KEY not found" {}))))

(defn send-query [{:keys [sql]}]
  (let [api-base "https://node-api.flipsidecrypto.com/queries"
        resp
        (http/request
          {:method  :post
           :uri     api-base
           :body    (json/write-str {:sql        sql
                                     :ttlMinutes 5})
           :headers {"Accept"       "application/json"
                     "Content-Type" "application/json"
                     "x-api-key"    (get-api-key)}})]
    (-> resp
        :body
        json/read-str
        (with-meta {::response resp}))))

;;; todo: pagination
(defn check-result [{:keys [token] :as pending-query}]
  (let [resp
        (http/request
          {:method       :get
           :uri          (str "https://node-api.flipsidecrypto.com/queries/"
                              token)
           :query-params {:pageNumber 1 :pageSize 10}
           :headers      {"Accept"       "application/json"
                          "Content-Type" "application/json"
                          "x-api-key"    (get-api-key)}
           :throw        false})]
    (-> resp
        :body
        json/read-str
        (with-meta {::response resp}))))
