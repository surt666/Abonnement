(ns Abonnement.servlet
  (:gen-class :extends javax.servlet.http.HttpServlet)
  (:require [compojure.route :as route])
  (:use ring.util.servlet
	[Abonnement.redisroutes :only [app]]))

(defservice app)