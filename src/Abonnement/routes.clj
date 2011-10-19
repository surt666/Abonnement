(ns Abonnement.routes
  (:use compojure.core
        ring.util.response
        Abonnement.core
        yousee-common.wrappers        
       ;; ring.commonrest
        yousee-common.web)
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [clojure.walk :as walk]
            [clojure.data.json :as json]))

(defn json-response [data content-type & {:as attrs}]
  "Data is the http body, :status is optional httpcode, :etag is optional calculated etag value and content-type is ex. application/vnd.yoursee+json. :cache-control and :expires are optional"    
  (let [res {:status (or (:status attrs) 200)
             :headers {"Content-Type" content-type 
                       "ETag" (str (if (:etag attrs) (:etag attrs) (hash data)))}
             :body (json/json-str data)}
        res2 (if (:cache-control attrs)
               (assoc-in res [:headers "Cache-Control"] (:cache-control attrs))
               res)
        res3 (if (:expires attrs)
               (assoc-in res2 [:headers "Expires"] (:expires attrs))
               res2)]
    res3))

(defn- opret-abonnement [req]  
  (let [body (parse-body (:body req))]
    (Abonnement.core.Abonnement. (:id body) (:juridiskaccount body) (:betaleraccount body) (:varenr body) (:status body) (:parent body) (:start body) (:opdateret body) (:historik body) (:meta body))))

(defroutes handler
  (POST "/abonnementer" req
       (let [data (opret (opret-abonnement req))
             status (if (= 204 (:status data) 200) (:status data))]
         (json-response (:abonnement data) "application/json;charset=UTF-8" :status status)))
  (PUT "/abonnementer" req
       (let [ifmatch (get (:headers req) "if-match")
             data (opret (opret-abonnement req) ifmatch)            
             status (if (= 204 (:status data)) 200 (:status data))]
         (json-response (:abonnement data) "application/json;charset=UTF-8" :status status)))
  (GET "/abonnementer/:id" [id]
       (let [abon (find-abon id)]
         (json-response (:abonnement abon) "application/json;charset=UTF-8" :etag (:etag abon) :expires "0" :cache-control "no-cache")))
  (DELETE "/abonnementer/:id" [id]
          (let [data (opsig id)]
            (json-response (:abonnement data) "application/json;charset=UTF-8" :status (:status data))))
  (GET "/abonnementer/installation/:amsid/:instnr" [amsid instnr]
       (let [data (find-alle-abon-for-amsid-og-instnr amsid instnr)
             status (if (empty? data) 404 200)]
         (json-response data "application/json;charset=UTF-8" :status status :expires "0" :cache-control "no-cache")))
  (GET "/abonnementer/juridisk/:id" [id]
       (let [data (find-alle-abon-for-account id)
             status (if (empty? data) 404 200)]
         (json-response data "application/json;charset=UTF-8" :status status :expires "0" :cache-control "no-cache")))
  (GET "/abonnementer/betaler/:id" [id]
       (let [data (find-alle-abon-for-account id "betaler")
             status (if (empty? data) 404 200)]
         (json-response data "application/json;charset=UTF-8" :status status :expires "0" :cache-control "no-cache")))

  (route/not-found "UPS det er jo helt forkert det der !"))

(def app
  (-> (handler/site handler)
      ;;(wrap-request-log-and-error-handling :body-str :body :status :server-port :query-string :server-name :uri :request-method :content-type :headers :json-params :params)
      )) 