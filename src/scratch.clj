(ns scratch
  (:require
   [clojure.pprint :refer [pprint]]
   [kafka-client :as kclient]
   [kafka-streams :as kstreams]
   [kafka-serdes :as kserdes]))

(def kc-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "test-group"
   "enable.auto.commit" "true"
   "auto.commit.interval.ms" "100"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

(def ks-config
  {"application.id" "scratch"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"
   "default.key.serde" (.getClass (kserdes/string-serde))
   "default.value.serde" (.getClass (kserdes/string-serde))
   "default.windowed.key.serde.inner" (.getClass (kserdes/string-serde))
   "default.windowed.value.serde.inner" (.getClass (kserdes/string-serde))})


(defn- make-ticks-stream
  [builder topic]
  (-> builder
      (kstreams/make-kstream topic (kserdes/string-serde) (kserdes/edn-serde))
      (kstreams/peek #(println topic ":>" %1 %2))
      (kstreams/map-values (fn [_] (get topic 0)))))

(defn make-ok-nok-pipeline
  [builder topic-name]
  (let [get-stream (kstreams/make-kstream builder
                                          topic-name
                                          (kserdes/string-serde)
                                          (kserdes/edn-serde))]

    (-> get-stream
        (kstreams/group-by-key)
        (kstreams/windowed-by 90 5)
        (kstreams/aggregate
         (fn [] {:ok-count 0, :nok-count 0})
         (fn [_ v acc]
           (case v
             200 (update acc :ok-count inc)
             (400 404 429 529 1529) (update acc :nok-count inc)
             acc))
         (kserdes/string-serde)
         (kserdes/edn-serde))
        (kstreams/ktable->kstream)
        (kstreams/peek #(println "counts:" %1 %2))
        (kstreams/filter (fn [_ v] (> (:nok-count v) 0)))
        (kstreams/map-values
         (fn [v] (double (/ (:ok-count v) (:nok-count v)))))
        (kstreams/peek #(println "ratio:" %1 %2))
        (kstreams/to "ratios"
                     (kstreams/windowed-string-serde)
                     (kserdes/double-serde)))))

(comment
  (def consumer (kclient/consumer kc-config))
  (def producer (kclient/producer kc-config (kserdes/string-serde) (kserdes/edn-serde)))
  (def builder (kstreams/make-streams-builder))
  (make-ok-nok-pipeline builder "fetches")
  (def kafka-streams (kstreams/start-kafka-streams builder ks-config))

  (kclient/subscribe consumer ["ratios"])

  (force (kclient/send! producer "fetches"  "user-1"
                        {:key "jon.doe@email.com" :status 200}))
  (force (kclient/send! producer "pushes" "user-1"
                        {:key "jon.doe@email.com" :value 100.00}))

  (pprint (kclient/poll consumer 1))

  (kstreams/stop-kafka-streams kstreams)

  (.close consumer)
  (.close producer))

(comment
  ;; Backup do do pipeline anterior
  (def kstreams
    (let [builder (kstreams/make-streams-builder)
          fetch-ticks (make-ticks-stream builder "fetches")
          push-ticks (make-ticks-stream builder "pushes")]

      (-> (kstreams/merge fetch-ticks push-ticks)
          (kstreams/group-by-key)
          (kstreams/windowed-by 30 5)
          (kstreams/aggregate
           (fn [] {:fetches-count 0, :pushes-count 0})
           (fn [_ v acc]
             (if (= v \f)
               (update acc :fetches-count inc)
               (update acc :pushes-count inc)))
           (kserdes/string-serde)
           (kserdes/edn-serde))
          (kstreams/ktable->kstream)
          (kstreams/peek #(println "counts:" %1 %2))
          (kstreams/filter (fn [_ v] (> (:fetches-count v) 0)))
          (kstreams/map-values
           (fn [v] (double (/ (:pushes-count v) (:fetches-count v)))))
          (kstreams/peek #(println "ratio:" %1 %2))
          (kstreams/to "ratios" (kstreams/windowed-string-serde) (kserdes/double-serde)))

      (kstreams/start-kafka-streams builder ks-config))))
