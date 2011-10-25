(ns jetty
  (:use ring.adapter.jetty)
  (:use Abonnement.redisroutes)  
  (:import (org.mortbay.xml XmlConfiguration)
	   (org.mortbay.jetty.webapp WebAppContext)))

(defn init-server [server]
  (try 
    (let [config (XmlConfiguration. (slurp "test/jetty.xml"))]   
      (. config configure server))
    (catch Exception e
      (prn "Unable to load jetty configuration")
      (. e printStackTrace))))


(defonce server
  (run-jetty #'app {:port 8080 :configurator init-server :join? false}))
