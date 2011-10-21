(ns Abonnement.core
  (:require [clojure.data.json :as json]
            [http.async.client :as http-client]
            [clj-time.core :as tc]
            [clj-time.format :as tf])
  (:import (java.util UUID)))

(def lb "http://riakloadbalancer-1546764266.eu-west-1.elb.amazonaws.com:8098")

(def abonnementer "abonnementer")

;(def lb "http://127.0.0.1:8098")

(declare find-abon)

(def date-formatter (tf/formatter "dd-MM-yyyy"))

(def datetime-formatter (tf/formatter "dd-MM-yyyy|hh:mm:ss"))

(defrecord Abonnement [id juridiskaccount betaleraccount varenr status parent start opdateret historik meta])

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
        historik (vec (:historik gl-abon))
        ny-historik (conj historik (assoc diff :dato (tf/unparse datetime-formatter (tc/now))))]
    (if (not (empty? diff))
      (assoc nyt-abon :historik ny-historik)
      nyt-abon)))

(defn riak-put [bucket rec etag]
  (with-open [client (http-client/create-client)]
    (let [uuid (.. UUID randomUUID toString)
          rec2 (if (nil? (:id rec)) (assoc rec :id uuid :opdateret (tf/unparse date-formatter (tc/now))) (assoc rec :opdateret (tf/unparse date-formatter (tc/now))))
          exist-rec (find-abon (:id rec2)) ;; check for eksisterende
          not-changed (= etag (:etag exist-rec))         
          rec-hist (if etag (opdater-historik rec2 (:abonnement exist-rec)) rec2)
          headers {:content-type "application/json"
                   :x-riak-index-amsinst_bin (str (get-in rec-hist [:meta :amsid]) ":" (get-in rec-hist [:meta :instnr]))
                   :x-riak-index-juridiskaccount_bin (:juridiskaccount rec-hist)
                   :x-riak-index-betaleraccount_bin (:betaleraccount rec-hist)}]
      (if not-changed
        (let [resp (http-client/PUT client (str lb "/buckets/" bucket "/keys/" (:id rec-hist) "?returnbody=false&w=2&dw=2")
                                    :body (json/json-str rec-hist)                          
                                    :headers headers
                                    :proxy {:host "sltarray02" :port 8080})]
          (wait-resp resp)        
          {:abonnement rec-hist
           :status (:code (http-client/status resp))})
        {:abonnement nil
         :status 412}))))

(defn riak-get [bucket id]  
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/GET client (str lb "/buckets/" bucket "/keys/" id "?returnbody=true&r=2") 
                                :headers {:content-type "application/json"}
                                :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)      
      (if (= 200 (:code (http-client/status resp)))
        (let [etag (json/read-json (:etag (http-client/headers resp)))]          
          [etag (json/read-json (http-client/string resp))])
        (http-client/status resp)))))

(defn opret [abon & etag]
  (riak-put abonnementer abon (first etag)))

(defn find-abon [id]
  (let [a (riak-get abonnementer id)     
        s (get a 1)]  
    {:etag (get a 0)
     :abonnement (Abonnement. (:id s) (:juridiskaccount s) (:betaleraccount s) (:varenr s)
                              (:status s) (:parent s) (:start s) (:opdateret s) (:historik s) (:meta s))}))

(defn opsig [id]
  (let [exist-rec (:abonnement (find-abon id))
        abon-opsagt (assoc exist-rec :status "opsagt")]    
    (riak-put abonnementer abon-opsagt))) 

(defn find-alle-abon-for-account [accid & abon-type]  
  (with-open [client (http-client/create-client)]
    (let [index (if (= (first abon-type) "betaler") "betaleraccount_bin" "juridiskaccount_bin")
          resp (http-client/POST client (str lb "/mapred")
                                 :body (json/json-str {:inputs {
                                                                :bucket abonnementer
                                                                :index index
                                                                :key accid    
                                                                }

                                                       :query [{:map {:language "javascript"  
                                                                      :name "Riak.mapValuesJson"   
                                                                      }
                                                                }
                                                               ]
                                                       })                                           
                                 :headers {:content-type "application/json"}
                                 :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)         
      (if (= 200 (:code (http-client/status resp)))
        (json/read-json (http-client/string resp))
        (http-client/status resp)))))

(defn find-alle-abon-for-amsid-og-instnr [amsid instnr]
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/POST client (str lb "/mapred")
                                 :body (json/json-str {:inputs {
                                                                :bucket abonnementer
                                                                :index "amsinst_bin"
                                                                :key (str amsid ":" instnr)    
                                                                }
                                                       :query [{:map {:language "javascript"
                                                                      :name "Riak.mapValuesJson"
                                                                      }
                                                                }
                                                               ]
                                                       })                                    
                                 :headers {:content-type "application/json"}
                                 :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)       
      (if (= 200 (:code (http-client/status resp)))
        (json/read-json (http-client/string resp))
        (http-client/status resp)))))

(defn find-antal-abonnementer []
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/POST client (str lb "/mapred")
                                 :body (json/json-str {:inputs abonnementer                               
                                                       :query [{:map {:language "javascript"
                                                                      :source "function mapCount() {
                                                                                  return [1];
                                                                               }"
                                                                      }
                                                                
                                                                }
                                                               {
                                                                :reduce {
                                                                         :language "javascript"
                                                                         :name "Riak.reduceSum"
                                                                         }
                                                                }
                                                               ]
                                                       })                                    
                                 :headers {:content-type "application/json"}
                                 :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)       
      (if (= 200 (:code (http-client/status resp)))
        (json/read-json (http-client/string resp))
        (http-client/status resp)))))