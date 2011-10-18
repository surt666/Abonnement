(ns Abonnement.core
  (:require [clojure.data.json :as json]
            [http.async.client :as http-client])
  (:import (java.util UUID)))

(def lb "http://riakloadbalancer-1546764266.eu-west-1.elb.amazonaws.com:8098")

(defrecord Abonnement [id juridiskaccount betaleraccount varenr status parent start opdateret historik meta])

(defn generate-abonnr []
  (str (rand-int 10000000)))

(defn wait-resp [resp]
  (when-not (http-client/done? resp)
    (http-client/await resp)
    (http-client/done? resp)))

(defn riak-put [bucket rec]
  (with-open [client (http-client/create-client)]
    (let [rec2 (when (nil? (:id rec)) (assoc rec :id (.. UUID randomUUID toString)))
          resp (http-client/PUT client (str lb "/buckets/" bucket "/keys/" (:id rec) "?returnbody=false&w=2&dw=2")
                                :body (json/json-str rec2)                          
                                :headers {:content-type "application/json"
                                          :x-riak-index-amsinst_bin (str (get-in rec [:meta :amsid]) ":" (get-in rec [:meta :instnr]))
                                          :x-riak-index-juridiskaccount_bin (:juridiskaccount rec)
                                          :x-riak-index-betaleraccount_bin (:betaleraccount rec)}
                                :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)
      (http-client/status resp))))

(defn riak-get [bucket rec]
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/GET client (str lb "/buckets/" bucket "/keys/" (:id rec) "?returnbody=true&r=2") 
                                :headers {:content-type "application/json"}
                                :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)
      (if (= 200 (:code (http-client/status resp)))
        (json/read-json (http-client/string resp))
        (http-client/status resp)))))


(defn opret [abon]
  (riak-put "abonnementer" abon))

(defn find-abon [abon]
  (let [s (riak-get "abonnementer" abon)]  
    (Abonnement. (:id s) (:juridiskaccount s) (:betaleraccount s) (:varenr s) (:status s) (:parent s) (:start s) (:opdateret s) (:historik s) (:meta s))))

(defn opsig [abon]
  (let [abon-opsagt (assoc abon :status "opsagt")]
    (riak-put "abonnementer" abon))) 

(defn find-alle-abon-for-account [abon & abon-type]  
  (with-open [client (http-client/create-client)]
    (let [index (if (= (first abon-type) "betaler") "betaleraccount_bin" "juridiskaccount_bin")
          key (if (= (first abon-type) "betaler") (:betaleraccount abon) (:juridiskaccount abon))
          resp (http-client/POST client (str lb "/mapred")
                                 :body (json/json-str {:inputs {
                                                                :bucket "abonnementer"
                                                                :index index
                                                                :key (str key)    
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
(defn find-alle-abon-for-amsid-og-instnr [abon]
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/POST client (str lb "/mapred")
                                 :body (json/json-str {:inputs {
                                                                :bucket "abonnementer"
                                                                :index "amsinst_bin"
                                                                :key (str (get-in abon [:meta :amsid]) ":" (get-in abon [:meta :instnr]))    
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