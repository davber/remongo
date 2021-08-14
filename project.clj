(defproject remongo "0.1.0"
  :description "ClojureScript library synchronizing Re-frame DB's and MongoDB via Realm"
  :url "http://example.com/FIXME"
  :license {:name "Unlicense"
            :url "https://unlicense.org/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.3.618"]
                 [com.taoensso/timbre "5.1.2" :exclusions [io.aviso/pretty com.taoensso/encore]]]
  :repl-options {:init-ns remongo.core})
