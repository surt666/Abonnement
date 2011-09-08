(defproject Abonnement "1.0.0"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojars.ossareh/clj-riak "0.1.0-SNAPSHOT"]
                 [compojure "0.6.4"]
                 [ring/ring-core "0.3.10" :exclusions [javax.servlet/servlet-api]]
                 [ring/ring-servlet "0.3.10" :exclusions [javax.servlet/servlet-api]]                 
                 [yousee-common "1.0.27"]
                 [ring-common "1.1.1"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :dev-dependencies [[ring/ring-jetty-adapter "0.3.10" :exclusions [org.mortbay.jetty/servlet-api]]
                     [ring/ring-devel "0.3.10"]
                     [uk.org.alienscience/leiningen-war "0.0.13"]]
  :aot [Abonnement.servlet]
  :war {:web-content "war-root"})
