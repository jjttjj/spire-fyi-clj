(ns fyi.spire.util
  (:require [clojure.string :as str]
            [selmer.parser :as parser]
            [tick.core :as t]))

(defn jinja->selmer [s]
  (-> s
      (str/replace #"-?%-?" "%")
      (str/replace #"loop\." "forloop.")))

(defn template-args [template]
  (-> template
      jinja->selmer
      parser/known-variables))

(defn render-query [template args]
  (-> template
      jinja->selmer
      (parser/render args)
      (selmer.util/without-escaping)))

(defn quote-sql-date [s]
  (format "'%s'" s))
(defn fmt-sql-date [inst]
  (t/format (t/formatter "yyyy-MM-dd HH:mm:ss") (t/date-time inst)))
