(ns ^{
      :author "Steen Larsen stel@yousee.dk"
      :doc "DB kald"
      }
  Abonnement.db
  (:use yousee-common.common)
  (:require [clojure.java.jdbc :as sql]            
            [clojure.data.json :as json]
            ;[http.async.client :as http-client]
            [Abonnement.redisabon :as ra])
  (:import (java.util UUID)))

(def edm {:classname "oracle.jdbc.driver.OracleDriver", :subprotocol "oracle:thin", :subname "@lisbon.tdk.dk:1521:tctvspoc", :user "k2_dw", :password "blaa"})

(defn get-all-subscriptions-for-amsno [amsno]
  (sql/with-connection edm
    (sql/with-query-results rs ["select *   
                                 from k2_addressproduct
                                 where amsno = ?
                                 and (enddate is null or enddate > sysdate)" amsno]
      (map #(convert-types %) (vec rs)))))

(defn get-all-subscriptions []
  (sql/with-connection edm
    (sql/with-query-results rs [{:fetch-size 1000} "select customerid,productid,agreementno,startdate,amsno,cableunitinstallationno,cableunitid,serieno,modemid
                                 from k2_addressproduct
                                 where (enddate is null or enddate > sysdate)"]
      (map #(convert-types %) (vec rs)))))

(defn generate-sub-map [sub]
  "Generer abonnement doc"
  {
   :id (.. UUID randomUUID toString)
   :juridiskaccount (:customerid sub)
   :betaleraccount (:customerid sub)
   :varenr (:productid sub)
   :status "aktiv"
   :parent (:agreementno sub)
   :start (:startdate sub)
   :opdateret (:startdate sub)
   :meta {
          :amsid (:amsno sub)
          :instnr (:cableunitinstallationno sub)
          :kontrakt (:cableunitid sub) 
          :juridisk (:customerid sub)
          :betaler (:customerid sub)
          :serienr [(:serieno sub)]
          :modemid (:modemid sub)
          :ordreid (rand-int 10000000)
          }
   })

(defn generate-redis-sub-map [sub]
  {
   "juridiskaccount" (:customerid sub)
   "betaleraccount" (:customerid sub)
   "varenr" (:productid sub)
   "status" "aktiv"
   "amsid" (str (:amsno sub))
   "instnr" (str (:cableunitinstallationno sub))
   "kontrakt" (str (:cableunitid sub)) 
   "juridisk" (:customerid sub)
   "betaler" (:customerid sub)
   "serienr" (str "[" (:serieno sub) "]")
   "aktiveringskode" (str (:modemid sub))
   "ordreid" (str (rand-int 10000000))
   })

(comment (defn insert-sub [sub]  
   (with-open [client (http-client/create-client)]
     (let [sub2 (generate-sub-map sub)
           url (str "http://riakloadbalancer-1546764266.eu-west-1.elb.amazonaws.com:8098/buckets/abonnementer/keys/" (:id sub2) "?returnbody=false")          
           resp (http-client/PUT client url
                                 :body (json/json-str sub2)                          
                                 :headers {:content-type "application/json"
                                           :x-riak-index-amsinst_bin (str (get-in sub2 [:meta :amsid]) ":" (get-in sub2 [:meta :instnr]))
                                           :x-riak-index-juridiskaccount_bin (:juridiskaccount sub2)
                                           :x-riak-index-betaleraccount_bin (:betaleraccount sub2)}
                                 :proxy {:host "sltarray02" :port 8080})]
       (when-not (http-client/done? resp)
         (http-client/await resp)
         (http-client/done? resp))
       ;; (http-client/status resp)
       ))))

(defn insert-sub-redis [sub]
  (let [sub2 (generate-redis-sub-map sub)]    
    (ra/opret sub2)))

(defn insert-subs-in-db []
  (let [subs (get-all-subscriptions)    
        res (doall (pmap #(insert-sub-redis %) subs))]   
    ;; [(count (filter #(= 204 %) res)) (count (filter #(not (= 204 %)) res))]
    nil))