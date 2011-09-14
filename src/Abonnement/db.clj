(ns ^{
      :author "Steen Larsen stel@yousee.dk"
      :doc "DB kald"
      }
  Abonnement.db
  (:use yousee-common.common)
  (:require [clojure.contrib.sql :as sql]
            [clj-riak.client :as riak]
            [clojure.contrib.json :as json]))

(def edm {:name "java:comp/env/jdbc/EDM"})

(defn get-all-subscriptions []
  (sql/with-connection edm
    (sql/with-query-results rs ["select *                                    
                                 from k2_addressproduct
                                 where (enddate is null or enddate > sysdate)"]
      (convert-types (first rs)))))

(defn generate-sub-map [sub]
  {
   :accountid (:customerid sub)
   :varenr (:productid sub)
   :status "active"
   :aftalenr (:agreementno sub)
   :startdato (:startdate sub)
   :meta {
          :amsid (:amsno sub)
          :instnr (:cableunitinstallationno sub)
          :juridisk (:customerid sub)
          :serienr (:serieno sub)
          :modemid (:modemid sub)
          :ordreid (rand-int 10000000)
          }
   })

(defn insert-sub [sub]
  (riak/put rc "abonnementer" (str (:amsno sub) "." (:cableunitinstallationno sub) "." (:customerid sub))
            {:value (.getBytes (json/json-str (generate-sub-map sub)))
             :content-type "application/json"}))

(defn insert-subs-in-riak []
  (let [subs (get-all-subscriptions)]
    (doall (map #(insert-sub %) subs))))