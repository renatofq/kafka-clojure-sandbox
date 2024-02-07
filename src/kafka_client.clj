(ns kafka-client
  "Clojure wrapper around Kafka Client API"
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.datafy :refer [datafy]])
  (:import java.time.Duration
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.common.serialization.Serde))

(defn producer
  (^KafkaProducer [config]
   (KafkaProducer. ^java.util.Map config))
  (^KafkaProducer [config key-serde value-serde]
   (KafkaProducer. ^java.util.Map config
                   (.serializer ^Serde key-serde)
                   (.serializer ^Serde value-serde))))

(defn send!
  [producer topic message-key message]
  (let [record (ProducerRecord. topic message-key message)
        future (.send ^KafkaProducer producer ^ProducerRecord record)]
    (delay (datafy @future))))

(defn consumer
  (^KafkaConsumer [config]
   (KafkaConsumer. ^java.util.Map config))
  (^KafkaConsumer [config key-serde value-serde]
   (KafkaConsumer. ^java.util.Map config
                   (.deserializer ^Serde key-serde)
                   (.deserializer ^Serde value-serde))))

(defn subscribe
  ^KafkaConsumer [^KafkaConsumer consumer topics]
  (.subscribe consumer topics)
  consumer)

(defn poll
  [^KafkaConsumer consumer timeout-seconds]
  (map datafy (.poll consumer (Duration/ofSeconds timeout-seconds))))

(comment
  (def config
    {"bootstrap.servers" "localhost:9092"
     "group.id" "test-group"
     "default.topic" "magic-topic"
     "max.poll.records" "10"
     "enable.auto.commit" "true"
     "auto.commit.interval.ms" "500"
     "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
     "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
     "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
     "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

  (def cli (producer config))

  (send! cli "topic" "2" "hello")
  (pprint cli)

  (def consu (consumer config))

  (subscribe consu ["topic"])
  (def recs (poll consu 10))
  (pprint recs)
  )
