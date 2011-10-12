(ns Abonnement.core
 (:require [clojure.data.json :as json]))

(def lb "http://riakloadbalancer-1546764266.eu-west-1.elb.amazonaws.com:8098")

(defprotocol AbonnementHandler
  (opret [abon])
  (opsig [abon])
  (find-abon [abon])
  (find-alle-abon-for-instnr [abon])
  (find-alle-abon-for-account [abon])
  (flyt [abon kunde]))

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
          resp (http-client/PUT client (str lb "/riak/" bucket "/" (:id rec) "?returnbody=false")
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
    (let [resp (http-client/GET client (str lb "/riak/" bucket "/" (:id rec) "?returnbody=true") 
                                :headers {:content-type "application/json"}
                                :proxy {:host "sltarray02" :port 8080})]
      (wait-resp resp)
      (if (= 200 (http-client/status resp))
        resp
        (http-client/status resp)))))

(extend-type Abonnement
  AbonnementHandler
  (opret [abon]
    (riak-put "abonnementer" abon))
  (find-abon [abon]
    (let [s (riak-get "abonnementer" abon)]
      (Abonnement. (:id abon) (:juridiskaccount abon) (:betaleraccount abon) (:varenr abon) (:status abon) (:parent abon) (:start abon) (:opdateret abon) (:historik abon) (:meta abon))))
  (opsig [abon]
    (let [abon-opsagt (assoc abon :status "opsagt")]
      (riak-put "abonnementer" abon))) 
  (find-alle-abon-for-juridiskaccount [abon]
    (with-open [client (http-client/create-client)]
        (let [resp (http-client/POST client (str lb "/mapred")
                                     :body {"inputs" "abonnementer"
                                            "query" [{"map" {"language" "javascript"
                                                             "source" "function(value,keydata,arg) {
                                                                         var o = JSON.parse(value.values[0].data);
                                                                         var a = o.juridiskaccount;
                                                                         var r = [];
                                                                         if (a == arg) {
                                                                            r.push(o);
                                                                         }
                                                                         return r;
                                                                       }"
                                                       "keep" true
                                                       "arg" (:juridiskaccount abon)}}]}
                                    :headers {:content-type "application/json"}
                                    :proxy {:host "sltarray02" :port 8080})]
          (wait-resp resp)
          (if (= 200 (http-client/status resp))
            (flatten (map #(json/read-json %) resp))
            (http-client/status resp)))))
  (find-alle-abon-for-instnr [abon]
    (with-open [client (http-client/create-client)]
        (let [resp (http-client/POST client (str lb "/mapred")
                                     :body {"inputs" "abonnementer"
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
                                                       "arg" (get-in abon [:meta :installation])}}]}
                                     :headers {:content-type "application/json"}
                                     :proxy {:host "sltarray02" :port 8080})]
          (wait-resp resp)
          (if (= 200 (http-client/status resp))
            (flatten (map #(json/read-json %) resp))
            (http-client/status resp))))))

(defn test-a []
  (let [abonnr (generate-abonnr)
        a1 (Abonnement. abonnr "603186920" "1201505" "aktiv" "1291298" "08-09-2011" {:installation "82732837" :serienr ["82781237" "8237827"] :ordreid "82378"})]
    ;; (opret a1)
    (find-abon a1)
    (find-alle-abon-for-account a1)
    ;; (find-alle-abon-for-instnr a1)
    ))