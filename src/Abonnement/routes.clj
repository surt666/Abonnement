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

(defn- opret-abonnement [req]  
  (let [body (parse-body (:body req))]
    (Abonnement.core.Abonnement. (:id body) (:juridiskaccount body) (:bataleraccount body) (:varenr body) (:status body) (:parent body) (:start body) (:opdateret body) (:historik body) (:meta body))))

(defroutes handler 
  (POST "/abonnementer" req              
        (opret (opret-abonnement req)))
  (PUT "/abonnementer" req
       (opret (opret-abonnement req)))
  (GET "/abonnementer/:id" [id]
       (find-abon (Abonnement.core.Abonnement. id nil nil nil nil nil nil nil nil nil)))
  (DELETE "/abonnemenert/:id" [id]
          (opsig (Abonnement.core.Abonnement. id nil nil nil nil nil nil nil nil nil)))
  (GET "/abonnementer/installation/:amsid/:instnr" [amsid instnr]
       (find-alle-abon-for-amsid-og-instnr amsid instnr))
  (GET "/abonnementer/juridisk/:id" [id]
       (find-alle-abon-for-account id))
  (GET "/abonnementer/betaler/:id" [id]
       (find-alle-abon-for-account id "betaler"))

  (route/not-found "UPS det er jo helt forkert det der !"))

(def app
  (-> (handler/site handler)
      (wrap-request-log-and-error-handling :body-str :body :status :server-port :query-string :server-name :uri :request-method :content-type :headers :json-params :params))) 