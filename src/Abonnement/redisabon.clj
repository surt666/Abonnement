(ns Abonnement.redisabon
  (:use clojure.walk
        digest)
  (:require [clojure.data.json :as json]
            [clj-time.core :as tc]
            [clj-time.format :as tf]
            [clj-redis.client :as redis]))

;;(def db (redis/init :url "redis://46.137.157.48"))

(def db1 (redis/init :url "redis://localhost:6379"))

(def db2 (redis/init :url "redis://localhost:6380"))

(def datetime-formatter (tf/formatter "dd-MM-yyyy|hh:mm:ss"))

(def ^:dynamic *ring* { ;; TODO some schema for more than one slave should be devised
                       :m0 db1 :s0 db2
                     ; :m1 db1 :s1 db2
                     ; :m2 db3 :s2 db4
                       })

(def node-depth 2)  ;; the number of nodes with same data 

(defn find-db [key & [write]]  
  (let [hash (read-string (str "0x" (sha1 key))) ;; sha1 is much more expensive than standard hash. Is it necessary ?
        nc (mod hash (/ (count (keys *ring*)) node-depth))]
    (if write
      (get *ring* (keyword (str "m" nc)))
      (loop [res nil]
        (if (not (nil? res))
          res
          (recur (let [key (get [(str "m" nc) (str "s" nc)] (rand-int node-depth))
                       db (get *ring* (keyword key))]                   
                   (try
                     (redis/ping db) ;; TODO this ping step should be made unneccasary
                     db
                     (catch Exception e
                       nil)))))))))

(defrecord Abonnement [id
                       juridiskaccount
                       betaleraccount
                       varenr
                       status
                       parent
                       start
                       amsid
                       instnr
                       kontrakt
                       ordreid
                       serienr
                       aktiveringskode
                       tlfnr
                       juridisk
                       betaler
                       historik])

(defn- map-difference [m1 m2]
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
        ny-historik (assoc diff :dato (tf/unparse datetime-formatter (tc/now)))]    
    (when (not (empty? diff))
      (let [key (str "abonnement:" (:id nyt-abon) ":historik")]
        (redis/lpush (find-db key true) key (json/json-str ny-historik))))))


(defn make-abon-map [abon]
  (loop [a (keys abon) res {}]
    (if (empty? a)
      res
      (recur (rest a) (if (nil? (get abon (first a)))
                        res
                        (assoc res (name (first a)) (get abon (first a))))))))

(defn- nyt-abonnr []
  (let [key "abonnement:nr"]
    (redis/incr (find-db key true) key)))

(defn- set-redis [abon ny]  ;; TODO should handle rollback if some key can't be written
  (let [id (if ny (str (nyt-abonnr)) (:id abon))
        abon-map-str (make-abon-map (if ny (assoc abon :id id :start (tf/unparse datetime-formatter (tc/now)) :historik (str "abonnement:" id ":historik")) abon))
        abon-map (keywordize-keys abon-map-str)]    
    (when (:juridiskaccount abon-map)
      (let [key (str "abonnement:" (:juridiskaccount abon-map) ":juridisk")]
        (redis/sadd (find-db key true) key (:id abon-map))))
    (when (:juridiskaccount abon-map)
      (let [key (str "abonnement:" (:betaleraccount abon-map) ":betaler")]
        (redis/sadd (find-db key true) key (:id abon-map))))
    (when (and (:amsid abon-map) (:instnr abon-map))
      (let [key (str "abonnement:" (:amsid abon-map) "." (:instnr abon-map) ":installation")]
        (redis/sadd (find-db key true) key (:id abon-map))))
    (when (:varenr abon-map)
      (let [key (str "abonnement:" (:varenr abon-map) ":varenr")]
        (redis/sadd (find-db key true) key (:id abon-map))))
    [id (let [key (str "abonnement:" (:id abon-map) ":abonnement")]
          (redis/hmset (find-db key true) key abon-map-str))])) 

(defn opret [abon] 
  {:pre [(not (nil? (:varenr abon)))
         (not (nil? (:juridiskaccount abon)))
         (not (nil? (:betaleraccount abon)))
         (not (nil? (:status abon)))
         (not (nil? (:ordreid abon)))]}
  (set-redis abon true))

(defn find-abon [id]
  (let [key (str "abonnement:" id ":abonnement")
        res (redis/hgetall (find-db key) key)
        ret (keywordize-keys (into {} res))]
    (assoc ret :serienr (json/read-json (:serienr ret)))))

(defn opdater [abon ifmatch]
  {:pre [(not (nil? (:varenr abon)))
         (not (nil? (:juridiskaccount abon)))
         (not (nil? (:betaleraccount abon)))
         (not (nil? (:status abon)))
         (not (nil? (:ordreid abon)))]}
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
    (map #(keywordize-keys (into {} (let [key (str "abonnement:" % ":abonnement")
                                          res (redis/hgetall (find-db key) key)
                                          ret (keywordize-keys (into {} res))]
                                      (assoc ret :serienr (json/read-json (:serienr ret)))))) abon-keys)))

(defn find-alle-abon-for-amsid-og-instnr [amsid instnr]
  (let [key (str "abonnement:" amsid "." instnr ":installation")
        abon-keys (redis/smembers (find-db key) key)]
    (map #(keywordize-keys (into {} (let [key (str "abonnement:" % ":abonnement")]
                                      (redis/hgetall (find-db key) key)))) abon-keys)))

(defn find-alle-abon-for-varenr [varenr]
  (let [key (str "abonnement:" varenr ":varenr")
        abon-keys (redis/smembers (find-db key) key)]
    (map #(keywordize-keys (into {} (let [key (str "abonnement:" % ":abonnement")]
                                      (redis/hgetall (find-db) key)))) abon-keys)))

(defn antal-keys []
  (count (redis/keys db2 "abonnement:*:abonnement")))