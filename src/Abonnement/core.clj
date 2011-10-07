(ns Abonnement.core
 (:require [clj-riak.client :as riak]
           [clojure.data.json :as json]))

(def rc (riak/init {:host "127.0.0.1" :port 8087}))

(defn ping-rc []
  (riak/ping rc))

(defprotocol AbonnementHandler
  (opret [abon])
  (opsig [abon])
  (find-abon [abon])
  (find-alle-abon-for-instnr [abon])
  (find-alle-abon-for-account [abon])
  (flyt [abon kunde]))

(defrecord Abonnement [id accountid varenr status aftalenr startdato meta])

(defn generate-abonnr []
  (str (rand-int 10000000)))

(defn riak-put [bucket rec]
  (riak/put rc bucket (:id rec)
            {:value (.getBytes (json/json-str rec))
             :content-type "application/json"}))

(defn riak-get [bucket rec]
  (let [res (riak/get rc bucket (:id rec))]
    (when res
      (json/read-json (String. (:value res))))))

(extend-type Abonnement
  AbonnementHandler
  (opret [abon]
    (riak-put "abonnementer" abon))
  (find-abon [abon]
    (let [s (riak-get "abonnementer" abon)]
      (Abonnement. (:id abon) (:accountid abon) (:varenr abon) (:status abon) (:aftalenr abon) (:startdato abon) (:meta abon))))
  (opsig [abon]
    (let [abon-opsagt (assoc abon :status "opsagt")]
      (riak-put "abonnementer" abon)))   ;http://localhost:8098/mapred
  (find-alle-abon-for-account [abon]
    (let [accountid (:accountid abon)
          ro (riak/map-reduce rc
                              {"inputs" "abonnementer"
                               "query" [{"map" {"language" "javascript"
                                                "source" "function(value,keydata,arg) {
                                                            var o = JSON.parse(value.values[0].data);
                                                            var a = o.accountid;
                                                            var r = [];
                                                            if (a == arg) {
                                                              r.push(o);
                                                            }
                                                            return r;
                                                          }"
                                                "keep" true
                                                "arg" accountid}}]})]
      (flatten (map #(json/read-json %) ro))))
  (find-alle-abon-for-instnr [abon]
    (let [instnr (:installation (:meta abon))          
          ro (riak/map-reduce rc
                              {"inputs" "abonnementer"
                               "query" [{"map" {"language" "javascript"
                                                "source" "function(value,keydata,arg) {
                                                            var o = JSON.parse(value.values[0].data);
                                                            var r = [];
                                                            if (o.meta.installation == arg) {
                                                              r.push(o);
                                                            }
                                                            return r;   
                                                          }"
                                                "keep" true
                                                "arg" instnr}}]})]
      (flatten (map #(json/read-json %) ro)))))

(defn test-a []
  (let [abonnr (generate-abonnr)
        a1 (Abonnement. abonnr "603186920" "1201505" "aktiv" "1291298" "08-09-2011" {:installation "82732837" :serienr ["82781237" "8237827"] :ordreid "82378"})]
    ;; (opret a1)
    (find-abon a1)
    (find-alle-abon-for-account a1)
    ;; (find-alle-abon-for-instnr a1)
    ))