(defproject Abonnement "1.0.0"
  :description "Abonnement system"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.1.0"]
                 [org.clojure/data.json "0.1.1"]   
                 [compojure "0.6.4"]
                 [org.clojars.retiman/clj-time "0.3.0"] 
                 [ring/ring-core "1.0.0-beta2" :exclusions [javax.servlet/servlet-api]]
                 [ring/ring-servlet "1.0.0-beta2" :exclusions [javax.servlet/servlet-api]]                 
                 [yousee-common "1.0.31" :exclusions [ring-common]]
                 [ring-common "1.1.7"]
                 [http.async.client "0.3.1"]
                 [org.slf4j/slf4j-simple "1.6.1"]
                 [clj-redis "0.0.13-SNAPSHOT"]
                ; [com.ashafa/clutch "0.2.5"]
                 [clj-http "0.1.2"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :dev-dependencies [[ring/ring-jetty-adapter "1.0.0-beta2" :exclusions [org.mortbay.jetty/servlet-api]]
                     [ring/ring-devel "1.0.0-beta2"]
                     [org.mortbay.jetty/jetty-plus "6.1.25"]
                     [org.mortbay.jetty/jetty-naming "6.1.25"]
                     [uk.org.alienscience/leiningen-war "0.0.13"]
                     [oracle/ojdbc "6"]
                     [yij/lein-plugins "1.0.2"]
                     [javax.servlet/servlet-api "2.5"]
                     [uk.org.alienscience/leiningen-war "0.0.13"]
                     [swank-clojure "1.3.3"]]
  :aot [Abonnement.servlet]
  :war {:web-content "war-root"}
  :jvm-opts ["-Xmx1500m"])
