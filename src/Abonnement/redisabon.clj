(ns Abonnement.redisabon
  (:use clojure.walk)
  (:require [clojure.data.json :as json]
            [clj-time.core :as tc]
            [clj-time.format :as tf]
            [clj-redis.client :as redis]))

;;(def db (redis/init :url "redis://46.137.157.48"))

(def db (redis/init :url "redis://localhost"))

(def datetime-formatter (tf/formatter "dd-MM-yyyy|hh:mm:ss"))

(def ^:dynamic *ring* {
           :0 (redis/init :url "redis://localhost:6379")
          ; :1 (redis/init :url "redis://localhost:6389")
           })

(defn find-db [key]
  (get *ring* (keyword (str (mod (hash key) (count (keys *ring*)))))))


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
  (let [diff (dissoc (map-difference nyt-abon gl-abon) :historik)
        historik (vector (:historik gl-abon))
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
  (let [key "abonnement:nr"]
    (redis/incr (find-db key) key)))

(defn set-redis [abon ny]
  (let [id (if ny (str (nyt-abonnr)) (:id abon))
        abon-map-str (make-abon-map (if ny (assoc abon :id id :start (tf/unparse datetime-formatter (tc/now)) :historik (str "abonnement:" id ":historik")) abon))
        abon-map (keywordize-keys abon-map-str)]    
    (when (:juridiskaccount abon-map)
      (let [key (str "abonnement:" (:juridiskaccount abon-map) ":juridisk")]
        (redis/sadd (find-db key) key (:id abon-map))))
    (when (:juridiskaccount abon-map)
      (let [key (str "abonnement:" (:betaleraccount abon-map) ":betaler")]
        (redis/sadd (find-db key) key (:id abon-map))))
    (when (and (:amsid abon-map) (:instnr abon-map))
      (let [key (str "abonnement:" (:amsid abon-map) "." (:instnr abon-map) ":installation")]
        (redis/sadd (find-db key) key (:id abon-map))))
    (when (:varenr abon-map)
      (let [key (str "abonnement:" (:varenr abon-map) ":varenr")]
        (redis/sadd (find-db key) key (:id abon-map))))
    [id (let [key (str "abonnement:" (:id abon-map))]
          (redis/hmset (find-db key) key abon-map-str))])) 

(defn opret [abon]
  (set-redis abon true))

(defn find-abon [id]
  (let [key (str "abonnement:" id)
        res (redis/hgetall (find-db key) key)]
    (keywordize-keys (into {} res))))

(defn opdater [abon ifmatch]
  (let [exist-abon (find-abon (:id abon))
        etag (str (hash exist-abon))]    
    (if (= etag ifmatch)
      (let [abon-hist (opdater-historik (keywordize-keys (make-abon-map abon)) exist-abon)]
        (set-redis abon false))
      "CHG")))

(defn opsig [id] 
  (let [key (str "abonnement:" id)]
    (redis/hset (find-db key) key "status" "opsagt")))

(defn find-alle-abon-for-account [accid & abon-type]
  (let [type (if (= (first abon-type) "betaler") "betaler" "juridisk")
        key (str "abonnement:" accid ":" type)
        abon-keys (redis/smembers (find-db key) key)]
    (map #(keywordize-keys (into {} (let [key (str "abonnement:" %)]
                                      (redis/hgetall (find-db key) key)))) abon-keys)))

(defn find-alle-abon-for-amsid-og-instnr [amsid instnr]
  (let [key (str "abonnement:" amsid "." instnr ":installation")
        abon-keys (redis/smembers (find-db key) key)]
    (map #(keywordize-keys (into {} (let [key (str "abonnement:" %)]
                                      (redis/hgetall (find-db key) key)))) abon-keys)))

(defn find-alle-abon-for-varenr [varenr]
  (let [key (str "abonnement:" varenr ":varenr")
        abon-keys (redis/smembers db key)]
    (map #(keywordize-keys (into {} (let [key (str "abonnement:" %)]
                                      (redis/hgetall (find-db) key)))) abon-keys)))

(defn antal-keys []
  (count (redis/keys db "abonnement:*")))