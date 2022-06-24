(ns remongo.utils
  "Utilities for making Remongo work and interact with easier"
  (:require [clojure.walk :as walk]))

(defn dissoc-in
  "The function that should exist in the standard library, to dissoc deeply"
  [m path]
  (if (empty? path)
    {}
    (let [path' (take (dec (count path)) path)
          last-segment (last path)]
      (update-in m path' dissoc last-segment))))

(defn string-object
  "Create a string from a JS object"
  [obj]
  (try (.stringify js/JSON obj)
       (catch js/Error _ obj)))

(defn stringify-keyword
  "Stringigy keywords but leave everything else"
  [obj]
  (if (keyword? obj) (name obj) obj))

(defn stringify
  "Stringify all keywords to strings in a structure, for JSON preparation"
  [struct]
  (walk/prewalk stringify-keyword struct))

(defn vconj
  "Conj ensuring it is a vector first"
  [coll item]
  (conj (vec coll) item))
