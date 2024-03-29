(ns Abonnement.redisroutes
  (:use compojure.core
        ring.util.response
        Abonnement.redisabon
        yousee-common.wrappers        
        ring.commonrest
        clojure.walk
        yousee-common.web)
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [clojure.walk :as walk]
            [clojure.data.json :as json]))

(defn- opret-abonnement [req]  
  (let [body (parse-body (:body req))]
    (Abonnement.redisabon.Abonnement. (:id body) (:juridiskaccount body) (:betaleraccount body) (:varenr body) (:status body) (:parent body) (:start body) (:amsid body) (:instnr body) (:kontrakt body) (:ordreid body) (:serienr body) (:aktiveringskode body) (:tlfnr body) (:juridisk body) (:betaler body) (:historik body))))

(defroutes handler
  (POST ["/:context" , :context #".[^/]*"] req
        (let [abon (opret-abonnement req)              
              res (opret abon)
             status (if (= "OK" (get res 1)) 200 400)]
          (json-response {:id (get res 0)} "application/json;charset=UTF-8" :status status)))
  (PUT ["/:context" , :context #".[^/]*"] req
       (let [ifmatch (get (:headers req) "if-match")
             abon (opret-abonnement req)
             res (opdater abon ifmatch)            
             status (cond
                     (= "OK" (get res 1)) 204
                     (= "CHG" res) 409
                     :default 400)]
         (json-response nil "application/json;charset=UTF-8" :status status)))
  (GET ["/:context/:id" , :context #".[^/]*"] [id]
       (let [abon (find-abon id)]
         (json-response abon "application/json;charset=UTF-8" :etag (hash abon) :expires "0" :cache-control "no-cache")))
  (DELETE ["/:context/:id" , :context #".[^/]*"] [id]
          (let [data (opsig id)]
            (json-response nil "application/json;charset=UTF-8" :status (if (or (= data 0) (= data 1)) 204 400))))
  (GET ["/:context/installation/:amsid/:instnr" , :context #".[^/]*"] [amsid instnr]
       (let [data (find-alle-abon-for-amsid-og-instnr amsid instnr)
             status (cond
                     (empty? data) 404                     
                     :default 200)]
         (json-response data "application/json;charset=UTF-8" :status status :expires "0" :cache-control "no-cache")))
  (GET ["/:context/juridisk/:id" , :context #".[^/]*"] [id]
       (let [data (find-alle-abon-for-account id)
             status (cond
                     (empty? data) 404                     
                     :default 200)]
         (json-response data "application/json;charset=UTF-8" :status status :expires "0" :cache-control "no-cache")))
  (GET ["/:context/betaler/:id" , :context #".[^/]*"] [id]
       (let [data (find-alle-abon-for-account id "betaler")
             status (cond
                     (empty? data) 404                     
                     :default 200)]
         (json-response data "application/json;charset=UTF-8" :status status :expires "0" :cache-control "no-cache")))

  (route/not-found "UPS det er jo helt forkert det der !"))

(def app
  (-> (handler/site handler)
    ;  (wrap-request-log-and-error-handling :body-str :body :status :server-port :query-string :server-name :uri :request-method :content-type :headers :json-params :params)
      )) 