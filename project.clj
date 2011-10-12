(defproject Abonnement "1.0.0"
  :description "Abonnement system"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.0.6"]
                 [org.clojure/data.json "0.1.1"]                 
                 [compojure "0.6.4"]
                 [ring/ring-core "0.3.10" :exclusions [javax.servlet/servlet-api]]
                 [ring/ring-servlet "0.3.10" :exclusions [javax.servlet/servlet-api]]                 
                 [yousee-common "1.0.28"]
                 [ring-common "1.1.1"]
                 [http.async.client "0.3.1"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :dev-dependencies [[ring/ring-jetty-adapter "0.3.10" :exclusions [org.mortbay.jetty/servlet-api]]
                     [ring/ring-devel "0.3.10"]
                     [org.mortbay.jetty/jetty-plus "6.1.14"]
                     [org.mortbay.jetty/jetty-naming "6.1.14"]
                     [uk.org.alienscience/leiningen-war "0.0.13"]
                     [oracle/ojdbc "6"]
                     [yij/lein-plugins "1.0.2"]
                     [javax.servlet/servlet-api "2.5"]
                     [uk.org.alienscience/leiningen-war "0.0.13"]
                     [swank-clojure "1.3.3"]]
  :aot [Abonnement.servlet]
  :war {:web-content "war-root"}
  :jvm-opts ["-Xmx1500m"])
