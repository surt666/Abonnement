(ns Abonnement.routes
  (:use compojure.core
        ring.util.response
        Abonnement.core
        yousee-common.wrappers        
        ;; ring.commonrest
        ;; yousee-common.web
        )
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

(defn- keywordize-json [b] 
  (if (not (empty? b))
    (walk/keywordize-keys (json/read-json b))
    nil))

(defn- parse-body [body]
  "Find ud af om vi er i app server eller unit test"
  (cond
    (map? body) (walk/keywordize-keys body)
    (instance? java.lang.String) (keywordize-json body)
    :default
    (keywordize-json (slurp body))))

(defn- opret-abonnement [req]  
  (let [body (parse-body (:body req))]
    (Abonnement.core.Abonnement. (:id body) (:juridiskaccount body) (:betaleraccount body) (:varenr body) (:status body) (:parent body) (:start body) (:opdateret body) (:historik body) (:meta body))))

(defroutes handler 
  (POST "/abonnementer" req              
        (json-response (opret (opret-abonnement req)) "application/json;charset=UTF-8"))
  (PUT "/abonnementer" req
       (json-response (opret (opret-abonnement req)) "application/json;charset=UTF-8"))
  (GET "/abonnementer/:id" [id]
       (let [abon (find-abon id)]
         (json-response (:abonnement abon) "application/json;charset=UTF-8" :etag (:etag abon))))
  (DELETE "/abonnemenert/:id" [id]
          (opsig id))
  (GET "/abonnementer/installation/:amsid/:instnr" [amsid instnr]
       (json-response (find-alle-abon-for-amsid-og-instnr amsid instnr) "application/json;charset=UTF-8"))
  (GET "/abonnementer/juridisk/:id" [id]
       (json-response (find-alle-abon-for-account id) "application/json;charset=UTF-8"))
  (GET "/abonnementer/betaler/:id" [id]
       (json-response (find-alle-abon-for-account id "betaler") "application/json;charset=UTF-8"))

  (route/not-found "UPS det er jo helt forkert det der !"))

(def app
  (-> (handler/site handler)
      ;; (wrap-request-log-and-error-handling :body-str :body :status :server-port :query-string :server-name :uri :request-method :content-type :headers :json-params :params)
      )) 