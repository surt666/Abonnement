(ns Abonnement.redisabon
  (:use clojure.walk)
  (:require [clojure.data.json :as json]
            [clj-time.core :as tc]
            [clj-time.format :as tf]
            [clj-redis.client :as redis]))

(def db (redis/init :url "redis://46.137.157.48"))

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
  (let [abon-map (make-abon-map (if ny (assoc abon :id (str (nyt-abonnr))) abon))]
    (when (:juridiskaccount abon)
      (redis/sadd db (str "abonnement:" (:juridiskaccount abon) ":juridisk") (:id abon)))
    (when (:juridiskaccount abon)
      (redis/sadd db (str "abonnement:" (:betaleraccount abon) ":betaler") (:id abon)))
    (when (and (:amsid abon) (:instnr abon))
      (redis/sadd db (str "abonnement:" (:amsid abon) "." (:instnr abon) ":installation") (:id abon)))
    (when (:varenr abon)
      (redis/sadd db (str "abonnement:" (:varenr abon) ":varenr") (:id abon)))
    (redis/hmset db (str "abonnement:" (:id abon)) abon-map)))

(defn opret [abon]
  (set-redis abon true))

(defn opdater [abon ifmatch]
  (let [etag (hash (find-abon (:id abon)))]
    (if (= etag ifmatch)
      (set-redis abon false)
      "CHG")))

(defn find-abon [id]
  (let [res (redis/hgetall db (str "abonnement:" id))]
    (keywordize-keys (into {} res))))

(defn opsig [id]
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