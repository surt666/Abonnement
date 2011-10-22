(ns Abonnement.routes
  (:use compojure.core
        ring.util.response
        Abonnement.redisabon
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
    (Abonnement.redisabon.Abonnement. (:id body) (:juridiskaccount body) (:betaleraccount body) (:varenr body) (:status body) (:parent body) (:start body) (:amsid body) (:instnr body) (:ordreid body) (:serienr body) (:aktiveringskode body) (:tlfnr body) (:juridisk body) (:betaler body) (:historik body))))

(defroutes handler
  (POST ["/:context" , :context #".[^/]*"] req
        (let [abon (opret-abonnement req)
              res (opret abon)
             status (if (= "OK" res) 200 400)]
         (json-response abon "application/json;charset=UTF-8" :status status)))
  (PUT ["/:context" , :context #".[^/]*"] req
       (let [ifmatch (get (:headers req) "if-match")
             abon (opret-abonnement req)
             res (opdater abon ifmatch)            
             status (cond
                     (= "OK" res) 200
                     (= "CHG" res) 412
                     :default 400)]
         (json-response abon "application/json;charset=UTF-8" :status status)))
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
      ;;(wrap-request-log-and-error-handling :body-str :body :status :server-port :query-string :server-name :uri :request-method :content-type :headers :json-params :params)
      )) 