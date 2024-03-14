(ns kafka-streams
  (:refer-clojure :exclude [filter count peek merge])
  (:import
   [java.time Duration]
   [org.apache.kafka.streams KafkaStreams StreamsBuilder]
   [org.apache.kafka.streams.kstream
    Aggregator Consumed ForeachAction Initializer JoinWindows KeyValueMapper
    Materialized Predicate Produced TimeWindows StreamJoined
    ValueJoiner ValueMapper WindowedSerdes]))

(defn- config->props
  [config]
  (doto (java.util.Properties.)
    (.putAll config)))

(defn make-streams-builder
  "Returns a new StreamsBuilder"
  ^StreamsBuilder
  []
  (StreamsBuilder.))

(defn make-kstream
  "Returns a KStream given a builder a topic and the key and value serde"
  [^StreamsBuilder builder topic key-serde value-serde]
  (.stream builder topic (Consumed/with key-serde value-serde)))

(defn make-ktable
  "Returns a KStream given a builder a topic and the key and value serde"
  [^StreamsBuilder builder topic key-serde value-serde]
  (.table builder topic (Consumed/with key-serde value-serde) (Materialized/with key-serde value-serde)))

(defn start-kafka-streams
  "Creates and starts a KafkaStreams instance given a builder a the configuration"
  [^StreamsBuilder builder config]
  (let [topology (.build  builder)
        kafka-streams (KafkaStreams. topology (config->props config))]
    (.start kafka-streams)
    (println (.toString (.describe topology)))
    kafka-streams))

(defn stop-kafka-streams
  [^KafkaStreams kafka-streams]
  (.close kafka-streams))

(defn to
  "Terminal operation that materializes a KStream or KTable to a topic"
  [ksource topic key-serde value-serde]
  (.to ksource topic (Produced/with key-serde value-serde)))

(defn peek
  [ksource action]
  (.peek ksource
         (reify ForeachAction
           (apply [_ k v] (action k v)))))

(defn filter
  [ksource predicate]
  (.filter ksource
           (reify Predicate
             (test [_ k v] (predicate k v)))))

(defn map-values
  [ksource value-mapper]
  (.mapValues ksource
              (reify ValueMapper
                (apply [_ v] (value-mapper v)))))

(defn merge
  [ksource-a ksource-b]
  (.merge ksource-a ksource-b))

(defn group-by-key
  [kgroupable]
  (.groupByKey kgroupable))

(defn windowed-by
  [kwindowable size-seconds advance-seconds]
  (let [size-duration (Duration/ofSeconds size-seconds)
        advance-duration (Duration/ofSeconds advance-seconds)
        window (.advanceBy (TimeWindows/ofSizeWithNoGrace size-duration)
                           advance-duration)]
    (.windowedBy kwindowable window)))

(defn count
  [kcountable key-serde value-serde]
  (.count
   kcountable
   (Materialized/with key-serde value-serde)))

(defn aggregate
  [ksource initializer aggregator key-serde value-serde]
  (.aggregate ksource
              (reify Initializer
                (apply [_] (initializer)))
              (reify Aggregator
                (apply [_ k v a] (aggregator k v a)))
              (Materialized/with key-serde value-serde)))

(defn join
  ([kjoinable-a kjoinable-b joiner]
   (.join kjoinable-a
          kjoinable-b
          (reify ValueJoiner
            (apply [_ a b] (joiner a b)))))
  ([kjoinable-a kjoinable-b joiner
    time-seconds grace-seconds
    key-serde value-serde other-value-serde]
   (.join kjoinable-a
          kjoinable-b
          (reify ValueJoiner
            (apply [_ a b] (joiner a b)))
          (JoinWindows/ofTimeDifferenceAndGrace
           (Duration/ofSeconds time-seconds)
           (Duration/ofSeconds grace-seconds))
          (StreamJoined/with key-serde value-serde other-value-serde))))

(defn ktable->kstream
  ([ktable]
   (.toStream ktable))
  ([ktable key-mapper]
   (.toStream ktable (reify KeyValueMapper
                       (apply [_ k v] (key-mapper k v))))))

(defn windowed-string-serde
  []
  (WindowedSerdes/timeWindowedSerdeFrom java.lang.String))
