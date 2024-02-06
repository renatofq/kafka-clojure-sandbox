(ns kafka-streams
  (:refer-clojure :exclude [filter count peek merge])
  (:import java.time.Duration
           [org.apache.kafka.streams
            KafkaStreams StreamsBuilder Topology]
           [org.apache.kafka.streams.kstream
            KStream KTable Consumed Produced Materialized StreamJoined
            Initializer Aggregator ValueMapper ValueJoiner KeyValueMapper
            Predicate ForeachAction
            JoinWindows SlidingWindows TimeWindows WindowedSerdes]))

(defn- config->props
  [config]
  (doto (java.util.Properties.)
    (.putAll config)))

(defn make-streams-builder
  []
  (StreamsBuilder.))

(defn make-kstream
  ^KStream [builder topic key-serde value-serde]
  (.stream builder topic (Consumed/with key-serde value-serde)))

(defn start-kafka-streams
  ^KafkaStreams [builder config]
  (let [topology (.build builder)
        kafka-streams (KafkaStreams. ^Topology topology (config->props config))]
    (.start kafka-streams)
    (println (.toString (.describe topology)))
    kafka-streams))

(defn stop-kafka-streams
  [kafka-streams]
  (.close kafka-streams))

(defn to
  [ksource topic key-serde value-serde]
  (.to ksource topic (Produced/with key-serde value-serde)))

(defn peek
  [ksource action]
  (.peek ksource
            (reify ForeachAction
              (apply [this k v] (action k v)))))

(defn filter
  [ksource predicate]
  (.filter ksource
           (reify Predicate
             (test [this k v] (predicate k v)))))

(defn map-values
  [ksource value-mapper]
  (.mapValues ksource
              (reify ValueMapper
                (apply [this v] (value-mapper v)))))

(defn merge
  [ksource-a ksource-b]
  (.merge ksource-a ksource-b))

(defn group-by-key
  [kgroupable]
  (.groupByKey kgroupable))

(defn windowed-by
  [kwindowable time-seconds grace-seconds]
  (.windowedBy
   kwindowable
   (SlidingWindows/ofTimeDifferenceAndGrace
    (Duration/ofSeconds time-seconds)
    (Duration/ofSeconds grace-seconds))))

(defn count
  [kcountable key-serde value-serde]
  (.count
   kcountable
   (Materialized/with key-serde value-serde)))

(defn aggregate
  [ksource initializer aggregator key-serde value-serde]
  (.aggregate ksource
              (reify Initializer
                (apply [this] (initializer)))
              (reify Aggregator
                (apply [this k v a] (aggregator k v a)))
              ;;(Materialized/as key-serde value-serde)
              ))

(defn join
  ([kjoinable-a kjoinable-b joiner]
   (.join kjoinable-a
          kjoinable-b
          (reify ValueJoiner
            (apply [this a b] (joiner a b)))))
  ([kjoinable-a kjoinable-b joiner
    time-seconds grace-seconds
    key-serde value-serde other-value-serde]
   (.join kjoinable-a
          kjoinable-b
          (reify ValueJoiner
            (apply [this a b] (joiner a b)))
          (JoinWindows/ofTimeDifferenceAndGrace
           (Duration/ofSeconds time-seconds)
           (Duration/ofSeconds grace-seconds))
          (StreamJoined/with key-serde value-serde other-value-serde))))

(defn ktable->kstream
  ([ktable]
   (.toStream ktable))
  ([ktable key-mapper]
   (.toStream ktable (reify KeyValueMapper
                       (apply [this k v] (key-mapper k v))))))

(defn windowed-string-serde
  []
  (WindowedSerdes/timeWindowedSerdeFrom java.lang.String))
