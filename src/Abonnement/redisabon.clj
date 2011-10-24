(ns Abonnement.redisabon
  (:use clojure.walk)
  (:require [clojure.data.json :as json]
            [clj-time.core :as tc]
            [clj-time.format :as tf]
            [clj-redis.client :as redis]))

(def db (redis/init :url "redis://46.137.157.48"))

(def datetime-formatter (tf/formatter "dd-MM-yyyy|hh:mm:ss"))

(defrecord Abonnement [id
                       juridiskaccount
                       betaleraccount
                       varenr
                       status
                       parent
                       start
                       amsid
                       instnr
                       ordreid
                       serienr
                       aktiveringskode
                       tlfnr
                       juridisk
                       betaler
                       historik])

(defn map-difference [m1 m2]
  (loop [m (transient {})
         ks (concat (keys m1) (keys m2))]
    (if-let [k (first ks)]
      (let [e1 (find m1 k)
            e2 (find m2 k)]
        (cond (and e1 e2 (not= (e1 1) (e2 1))) (recur (assoc! m k (e1 1)) (next ks))
              (not e1) (recur (assoc! m k (e2 1)) (next ks))
              (not e2) (recur (assoc! m k (e1 1)) (next ks))
              :else    (recur m (next ks))))
      (persistent! m))))

(defn- opdater-historik [nyt-abon gl-abon] 
  (let [diff (dissoc (map-difference nyt-abon gl-abon) :historik :opdateret)
        historik (vec (:historik gl-abon))
        ny-historik (conj historik (assoc diff :dato (tf/unparse datetime-formatter (tc/now))))]
    (when (not (empty? diff))
      (redis/lpush db (str "abonnement:" (:id nyt-abon) ":historik") (json/json-str ny-historik)))))


(defn make-abon-map [abon]
  (loop [a (keys abon) res {}]
    (if (empty? a)
      res
      (recur (rest a) (if (nil? (get abon (first a)))
                        res
                        (assoc res (name (first a)) (get abon (first a))))))))

(defn nyt-abonnr []
  (redis/incr db "abonnement:nr"))

(defn set-redis [abon ny]
  (let [id (if ny (str (nyt-abonnr)) (:id abon))
        abon-map (make-abon-map (if ny (assoc abon :id id) abon))]
    (when (:juridiskaccount abon-map)
      (redis/sadd db (str "abonnement:" (:juridiskaccount abon-map) ":juridisk") (:id abon-map)))
    (when (:juridiskaccount abon-map)
      (redis/sadd db (str "abonnement:" (:betaleraccount abon-map) ":betaler") (:id abon-map)))
    (when (and (:amsid abon-map) (:instnr abon-map))
      (redis/sadd db (str "abonnement:" (:amsid abon-map) "." (:instnr abon-map) ":installation") (:id abon-map)))
    (when (:varenr abon-map)
      (redis/sadd db (str "abonnement:" (:varenr abon-map) ":varenr") (:id abon-map)))
    [id (redis/hmset db (str "abonnement:" (:id abon-map)) abon-map)]))

(defn opret [abon]
  (set-redis (assoc abon :start (tf/unparse datetime-formatter (tc/now)) :historik (str "abonnement:" (:id abon) ":historik")) true))

(defn find-abon [id]
  (let [res (redis/hgetall db (str "abonnement:" id))]
    (keywordize-keys (into {} res))))

(defn opdater [abon ifmatch]
  (let [exist-abon (find-abon (:id abon))
        etag (hash exist-abon)]
    (if (= etag ifmatch)
      (let [abon-hist (opdater-historik (make-abon-map abon) exist-abon)]
        (set-redis abon false))
      "CHG")))

(defn opsig [id] ;; TODO mangler historik
  (redis/hset db (str "abonnement:" id) "status" "opsagt"))

(defn find-alle-abon-for-account [accid & abon-type]
  (let [type (if (= (first abon-type) "betaler") "betaler" "juridisk")
        abon-keys (redis/smembers db (str "abonnement:" accid ":" type))]
    (map #(keywordize-keys (into {} (redis/hgetall db (str "abonnement:" %)))) abon-keys)))

(defn find-alle-abon-for-amsid-og-instnr [amsid instnr]
  (let [abon-keys (redis/smembers db (str "abonnement:" amsid "." instnr ":installation"))]
    (map #(keywordize-keys (into {} (redis/hgetall db (str "abonnement:" %)))) abon-keys)))

(defn find-alle-abon-for-varenr [varenr]
  (let [abon-keys (redis/smembers db (str "abonnement:" varenr ":varenr"))]
    (map #(keywordize-keys (into {} (redis/hgetall db (str "abonnement:" %)))) abon-keys)))