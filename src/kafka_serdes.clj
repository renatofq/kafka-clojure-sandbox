(ns kafka-serdes
  (:require [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization Serde Serdes Serializer Deserializer]))

(defn- edn-serializer []
  (reify Serializer
    (serialize [this topic data]
      (nippy/freeze data))))

(defn- edn-deserializer []
  (reify Deserializer
    (deserialize [this topic data]
      (nippy/thaw data))))

(defn edn-serde []
  (reify Serde
    (serializer [this]
      (edn-serializer))
    (deserializer [this]
      (edn-deserializer))))

(defn string-serde []
  (Serdes/String))

(defn double-serde []
  (Serdes/Double))

(defn long-serde []
  (Serdes/Long))
