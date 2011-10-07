(ns ^{
      :author "Steen Larsen stel@yousee.dk"
      :doc "DB kald"
      }
  Abonnement.db
  (:use yousee-common.common)
  (:require [clojure.java.jdbc :as sql]
            [clj-riak.client :as riak]
            [clojure.data.json :as json]
            [http.async.client :as http-client])
  (:import (java.util UUID)))

(def rcdb (riak/init {:host "localhost" :port 8087}))

(def edm {:classname "oracle.jdbc.driver.OracleDriver", :subprotocol "oracle:thin", :subname "@lisbon.tdk.dk:1521:tctvspoc", :user "k2_dw", :password "K2_DW_ON_SPOC"})

(defn get-all-subscriptions-for-amsno [amsno]
  (sql/with-connection edm
    (sql/with-query-results rs ["select *                                    
                                 from k2_addressproduct
                                 where amsno = ?
                                 and (enddate is null or enddate > sysdate)" amsno]
      (map #(convert-types %) (vec rs)))))

(defn get-all-subscriptions []
  (sql/with-connection edm
    (sql/with-query-results rs [{:fetch-size 10000} "select *                                    
                                 from k2_addressproduct
                                 where (enddate is null or enddate > sysdate)"]
      (map #(convert-types %) (vec rs)))))

(defn generate-sub-map [sub]
  {
   :id (.. UUID randomUUID toString)
   :juridiskaccount (:customerid sub)
   :betaleraccount (:customerid sub)
   :varenr (:productid sub)
   :status "active"
   :aftalenr (:agreementno sub)
   :startdato (:startdate sub)
   :meta {
          :amsid (:amsno sub)
          :instnr (:cableunitinstallationno sub)
          :juridisk (:customerid sub)
          :betaler (:customerid sub)
          :serienr (:serieno sub)
          :modemid (:modemid sub)
          :ordreid (rand-int 10000000)
          }
   })

(defn insert-sub [sub]
  (with-open [client (http-client/create-client)]
    (let [resp (http-client/PUT client (str "http://riakloadbalancer-1546764266.eu-west-1.elb.amazonaws.com:8098/riak/abonnementer/" (:amsno sub) "." (:cableunitinstallationno sub) "." (:customerid sub) "." (:agreementno sub) "." (:productid sub) "?returnbody=false")
                                :body (json/json-str (generate-sub-map sub))                          
                                :headers {:content-type "application/json"
                                          :x-riak-index-amsinst_bin (str (:amsno sub) ":" (:cableunitinstallationno sub))
                                          :x-riak-index-juridiskaccount_bin (:customerid sub)}
                                :proxy {:host "sltarray02" :port 8080})]
      (when-not (http-client/done? resp)
        (http-client/await resp)
        (http-client/done? resp))
      (http-client/status resp))))

(defn insert-subs-in-riak []
  (let [subs (get-all-subscriptions)    
        res (pmap #(:code (insert-sub %)) subs)]
    [(count (filter #(= 204 %) res)) (count (filter #(not (= 204 %)) res))]))