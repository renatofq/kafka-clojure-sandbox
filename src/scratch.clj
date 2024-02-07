(ns scratch
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [kafka-client :as kc]
   [kafka-streams :as ks]
   [kafka-serdes :as ksd]))

(def kc-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "test-group"
   "enable.auto.commit" "true"
   "auto.commit.interval.ms" "100"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

(def producer (kc/producer kc-config))
(force (kc/send! producer "consultas"  "user-1" "-"))
(force (kc/send! producer "pagamentos" "user-1" "100.00"))

(def ks-config
  {"application.id" "scratch"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"
   "default.key.serde" (.getClass (ksd/string-serde))
   "default.value.serde" (.getClass (ksd/string-serde))
   "default.windowed.key.serde.inner" (.getClass (ksd/string-serde))
   "default.windowed.value.serde.inner" (.getClass (ksd/string-serde))})


(defn- make-ticks-stream
  [builder topic]
  (-> builder
      (ks/make-kstream topic (ksd/string-serde) (ksd/string-serde))
      (ks/peek #(println topic ":>" %1 %2))
      (ks/map-values (fn [v] (get topic 0)))))

(def kstreams
  (let [builder (ks/make-streams-builder)
        consulta-ticks (make-ticks-stream builder "consultas")
        pagamento-ticks (make-ticks-stream builder "pagamentos")]

    (-> (ks/merge consulta-ticks pagamento-ticks)
        (ks/group-by-key)
        (ks/windowed-by 30 5)
        (ks/aggregate
         (fn [] "1,1")
         (fn [k v acc]
           (let [[c-count p-count] (str/split acc #"," 2)]
             (if (= v \c)
               (str (inc (Long/valueOf c-count)) \, p-count)
               (str c-count \, (inc (Long/valueOf p-count))))))
         (ksd/string-serde)
         (ksd/string-serde))
        (ks/ktable->kstream (fn [k v] (.key k)))
        (ks/peek #(println "merged-count:" %1 %2))
        (ks/map-values
         (fn [v]
           (let [[c-count p-count] (str/split v #",")]
             (double (/ (Long/valueOf c-count) (Long/valueOf p-count))))))
        (ks/peek #(println "saida:" %1 %2))
        (ks/to "saida" (ksd/string-serde) (ksd/double-serde)))

    (ks/start-kafka-streams builder ks-config)))

(comment
  (def consumer (kc/consumer kc-config))
  (kc/subscribe consumer ["saida"])

  (pprint (force (kc/send! producer "consultas"  "user-1" "-")))
  (pprint (force (kc/send! producer "pagamentos" "user-1" "100.00")))

  (pprint (kc/poll consumer 1))

  (ks/stop-kafka-streams kstreams)

  (.close consumer)
  (.close producer)
)
