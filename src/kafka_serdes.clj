(ns kafka-serdes
  (:require [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization Serde Serdes Serializer Deserializer]))

(defn- edn-serializer []
  (reify Serializer
    (serialize [_ _ data]
      (nippy/freeze data))))

(defn- edn-deserializer []
  (reify Deserializer
    (deserialize [_ _ data]
      (nippy/thaw data))))

(defn edn-serde []
  (reify Serde
    (serializer [_]
      (edn-serializer))
    (deserializer [_]
      (edn-deserializer))))

(defn string-serde []
  (Serdes/String))

(defn double-serde []
  (Serdes/Double))

(defn long-serde []
  (Serdes/Long))
