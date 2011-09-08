(ns Abonnement.servlet
  (:gen-class :extends javax.servlet.http.HttpServlet)
  (:require [compojure.route :as route])
  (:use ring.util.servlet
	[Abonnement.routes :only [app]]))

(defservice app)