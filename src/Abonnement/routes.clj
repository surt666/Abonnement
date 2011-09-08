(ns Abonnement.routes
  (:use compojure.core
        ring.util.response
        Abonnement.core
        yousee-common.wrappers
        ring.commonrest
        yousee-common.web)
  (:require [compojure.route :as route]
            [compojure.handler :as handler]            
            [clojure.contrib.json :as json]))

(defn- generate-aftalenr []
  "129291289")

(defn- opret-abonnement [body]
  (if (nil? (:aftalenr body))
    (Abonnement.core.Abonnement. (:accountid body) (:varenr body) (:status body) (generate-aftalenr) (:startdato body) (:meta body))
    (Abonnement.core.Abonnement. (:accountid body) (:varenr body) (:status body) (:aftalenr body) (:startdato body) (:meta body))))

(defroutes handler 
  (POST "/abonnement/opret" req      
        (let [body (parse-body (:body req))
              objekt (opret-abonnement body)]          
          (try
            (opret objekt)
            {:status 201}           
            (catch Exception e
              (prn "E" (.getMessage e))
              (json-response {:res (.getMessage e)} "application/json" :status 409)))))

  (route/not-found "UPS det er jo helt forkert det der !"))

(def app
  (-> (handler/site handler)
      (wrap-request-log-and-error-handling :body-str :body :status :server-port :query-string :server-name :uri :request-method :content-type :headers :json-params :params))) 