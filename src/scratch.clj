(ns dummy
  (:require [kafka.client :as kc]
            [kafka.streams :as ks]
            [clojure.pprint :refer [pprint]]
            [clojure.datafy :refer [datafy]]))

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
(pprint producer)

(def consumer (kc/consumer kc-config))
(kc/subscribe consumer ["cxp"])


(def kstreams
  (do
    (def ks-config
      {"application.id" "scratch"
       "bootstrap.servers" "localhost:9092"
       "cache.max.bytes.buffering" "0"})

    (def builder (ks/make-streams-builder))
    (def queryCount
      (-> builder
          (ks/make-kstream "consultas" (kc/string-serde) (kc/string-serde))
          (ks/peek #(println "consulta:" %1 %2))
          (ks/group-by-key)
          (ks/windowed-by 30 5)
          (ks/count (kc/string-serde) (kc/long-serde))))

    (def paymentCount
      (-> builder
          (ks/make-kstream "pagamentos" (kc/string-serde) (kc/string-serde))
          (ks/peek #(println "pagamento:" %1 %2))
          (ks/group-by-key)
          (ks/windowed-by 30 5)
          (ks/count (kc/string-serde) (kc/long-serde))))

    (-> (ks/join queryCount
                 paymentCount
                 (fn [queries payments] (double (/ queries payments))))
        (ks/ktable->kstream)
        (ks/peek #(println "saida:" %1 %2))
        ;;(ks/filter (fn [k v] (> v 1.2)))
        (ks/to "cxp" (ks/windowed-string-serde) (kc/double-serde)))

    (ks/make-kafka-streams builder ks-config)))

(comment
  (pprint (force (kc/send! producer "consultas"  "user-1" "-")))
  (pprint (force (kc/send! producer "pagamentos" "user-1" "100.00")))

  (pprint (kc/poll consumer 1))


  (pprint kstreams)

  (.close kstreams)
  (.close consumer)
  (.close producer))
