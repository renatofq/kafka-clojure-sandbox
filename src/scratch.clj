(ns user
  (:require [kafka.client :as kc]
            [kafka.streams :as ks]
            [clojure.pprint :refer [pprint]]
            [clojure.datafy :refer [datafy]])
  (:import org.apache.kafka.common.serialization.Serdes))

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
(kc/subscribe consumer ["saida"])


(def kstreams
  (do
    (def ks-config
      {"application.id" "scratch"
       "bootstrap.servers" "localhost:9092"
       "cache.max.bytes.buffering" "0"
       "default.key.serde" (.getClass (Serdes/String))
       "default.value.serde" (.getClass (Serdes/String))
       "default.windowed.key.serde.inner" (.getClass (Serdes/String))
       "default.windowed.value.serde.inner" (.getClass (Serdes/String))})

    (def builder (ks/make-streams-builder))
    (def consulta-ticks
      (-> builder
          (ks/make-kstream "consultas" (kc/string-serde) (kc/string-serde))
          (ks/peek #(println "consulta:" %1 %2))
          (ks/map-values (fn [v] "c"))))

    (def pagamento-ticks
      (-> builder
          (ks/make-kstream "pagamentos" (kc/string-serde) (kc/string-serde))
          (ks/peek #(println "pagamento:" %1 %2))
          (ks/map-values (fn [v] "p"))))

    (-> (ks/merge consulta-ticks pagamento-ticks)
        (ks/group-by-key)
        (ks/windowed-by 30 5)
        (ks/aggregate
         #("1,1")
         (fn [k v a] (a))
         ;; (fn [k v acc]
         ;;   (let [splited (clojure.string/split acc #"," 2)
         ;;         [c-count p-count] (mapv #(Integer/valueOf %) splited)]
         ;;     (clojure.string/join ","
         ;;                          (if (= v "c")
         ;;                            [(inc c-count) p-count]
         ;;                            [c-count (inc p-count)]))))
         (kc/string-serde)
         (kc/string-serde))
        ;; (ks/count (kc/string-serde) (kc/long-serde))
        (ks/ktable->kstream (fn [k v] (.key k)))
        ;;(ks/peek #(println "merged-count:" %1 %2))
        ;;(ks/map-values (fn [v] (double (apply / clojure.string/split))))
        (ks/peek #(println "saida:" %1 %2))
        (ks/to "saida" (kc/string-serde) (kc/string-serde)))

    (ks/start-kafka-streams builder ks-config)))

(comment
  (pprint (force (kc/send! producer "consultas"  "user-1" "-")))
  (pprint (force (kc/send! producer "pagamentos" "user-1" "100.00")))

  (pprint (kc/poll consumer 1))


  (pprint kstreams)

  (.close kstreams)
  (.close consumer)
  (.close producer)

  (reduce
   (fn [acc v]
     (let [splited (clojure.string/split acc #"," 2)
           [c-count p-count] (mapv #(Integer/valueOf %) splited)]
       (clojure.string/join
        ","
        (if (= v "c")
          [(inc c-count) p-count]
          [c-count (inc p-count)]))))
   "0,0"
   ["c" "c" "c" "v"])

  (reduce #("a") (fn [a b] b) ["b" "c"])

  (let [[c p] (mapv #(Integer/valueOf %) (clojure.string/split "0,0" #"," 2))]
    (println (clojure.string/join "," [c p])))

  
  )
