(ns kafka.streams
  (:refer-clojure :exclude [filter count peek])
  (:import java.time.Duration
           [org.apache.kafka.streams
            KafkaStreams StreamsBuilder Topology]
           [org.apache.kafka.streams.kstream
            KStream KTable Consumed Produced Materialized
            Predicate ValueJoiner ForeachAction SlidingWindows]))

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

(defn make-kafka-streams
  ^KafkaStreams [builder config]
  (let [kafka-streams
        (KafkaStreams. ^Topology (.build builder) (config->props config))]
    (.start kafka-streams)
    kafka-streams))

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

(defn group-by-key
  [kgroupable]
  (.groupByKey kgroupable))

(defn windowed-by
  [kwindowable time-seconds grace-seconds]
  (.windowedBy
   kwindowable
   (SlidingWindows/withTimeDifferenceAndGrace
    (Duration/ofSeconds time-seconds)
    (Duration/ofSeconds grace-seconds))))

(defn count
  [kcountable key-serde value-serde]
  (.count
   kcountable
   (Materialized/with key-serde value-serde)))

(defn join
  [kjoinable-a kjoinable-b joiner]
  (.join kjoinable-a
         kjoinable-b
         (reify ValueJoiner
           (apply [this a b] (joiner a b)))))

(defn ktable->kstream
  [ktable]
  (.toStream ktable))

(comment
  (reify
    ValueJoiner
    (apply [this a b] (println a b)))
  )
