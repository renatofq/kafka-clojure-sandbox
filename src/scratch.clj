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

(def producer (kc/producer kc-config (ksd/string-serde) (ksd/edn-serde)))
(force (kc/send! producer "consultas" "user-1" {:key "john.doe@email.com" :status 200}))
(force (kc/send! producer "pagamentos" "user-1" {:value 100.00}))

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
      (ks/make-kstream topic (ksd/string-serde) (ksd/edn-serde))
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
         (fn [] {:query-count 0, :payment-count 0})
         (fn [k v acc]
           (if (= v \c)
             (update acc :query-count inc)
             (update acc :payment-count inc)))
         (ksd/string-serde)
         (ksd/edn-serde))
        (ks/ktable->kstream
         ;; (fn [k v] (.key k))
         )
        (ks/peek #(println "counts:" %1 %2))
        (ks/filter (fn [k v] (> (:query-count v) 0)))
        (ks/map-values
         (fn [v] (double (/ (:payment-count v) (:query-count v)))))
        (ks/peek #(println "saida:" %1 %2))
        (ks/to "saida" (ks/windowed-string-serde) (ksd/double-serde)))

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
