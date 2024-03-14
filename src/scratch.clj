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

(def producer (kclient/producer kc-config (kserdes/string-serde) (kserdes/edn-serde)))
(force (kclient/send! producer "fetches" "user-1"
                 {:key "jon.doe@email.com" :status 200}))
(force (kclient/send! producer "pushes"  "user-1"
                 {:key "jon.doe@email.com" :value 100.00}))

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

    (kstreams/start-kafka-streams builder ks-config)))

(comment
  (def consumer (kclient/consumer kc-config))
  (kclient/subscribe consumer ["ratios"])

  (pprint (force (kclient/send! producer "fetches"  "user-1"
                           {:key "jon.doe@email.com" :status 200})))
  (pprint (force (kclient/send! producer "pushes" "user-1"
                           {:key "jon.doe@email.com" :value 100.00})))

  (pprint (kclient/poll consumer 1))

  (kstreams/stop-kafka-streams kstreams)

  (.close consumer)
  (.close producer)
)
