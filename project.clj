(defproject org.clojars.davber/remongo "0.2.10"
  :description "ClojureScript library synchronizing Re-frame DB's and MongoDB via Realm"
  :url "https://clojars.org/org.clojars.davber/remongo"
  :license {:name "Unlicense"
            :url "https://unlicense.org/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.5.640"]
                 [com.taoensso/timbre "5.1.2" :exclusions [io.aviso/pretty com.taoensso/encore]]]
  :repl-options {:init-ns remongo.mongo})
