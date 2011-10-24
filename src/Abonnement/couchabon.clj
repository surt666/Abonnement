(ns Abonnement.couchabon
  (:use clojure.walk)
  (:require [clojure.data.json :as json]
            [clj-time.core :as tc]
            [clj-time.format :as tf]
            [http.async.client :as http-client]
            ;; [clj-http.client :as client]
            ;; [com.ashafa.clutch :as clutch]
            )
  (:import (java.util UUID)))

(def db "https://surt666:madball8@surt666.cloudant.com/abonnementer")

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

(defn wait-resp [resp]
  (when-not (http-client/done? resp)
    (http-client/await resp)
    (http-client/done? resp)))

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
        historik (vec (:historik gl-abon))]
    (conj historik (assoc diff :dato (tf/unparse datetime-formatter (tc/now))))))

(defn opret [abon]
  (let [uuid (.. UUID randomUUID toString)]
    (with-open [client (http-client/create-client)]
      (let [resp (http-client/PUT client (str db "/" uuid)
                                    :body (json/json-str (assoc abon :start (tf/unparse datetime-formatter (tc/now)) :id uuid))                          
                                    :headers {:content-type "application/json"}
                                    :proxy {:host "sltarray02" :port 8080})]
          (wait-resp resp)                  
          (:code (http-client/status resp))))    
    ;; (clutch/with-db db
    ;;   (clutch/create-document (assoc abon :start (tf/unparse datetime-formatter (tc/now)) :id uuid) uuid))
    ))

(defn find-abon [id]
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/GET client (str db "/" id)                                
                                :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)      
      (if (= 200 (:code (http-client/status resp)))        
        (json/read-json (http-client/string resp))
        (http-client/status resp))))  
  ;; (clutch/with-db db
  ;;   (clutch/get-document id))
  )

(comment (defn opdater [abon]
           (let [exist-abon (find-abon (:id abon))
                 historik (opdater-historik abon exist-abon)
                 diff (map-difference (dissoc abon :historik) (dissoc exist-abon :historik))]
             (clutch/with-db db
               (clutch/update-document exist-abon (assoc diff :historik historik)))))

         (defn opsig [id]
           (let [exist-abon (find-abon id)
                 abon (assoc exist-abon :status "opsagt")
                 historik (opdater-historik abon exist-abon)]
             (clutch/with-db db
               (clutch/update-document exist-abon {:historik historik :status "opsagt"})))))

