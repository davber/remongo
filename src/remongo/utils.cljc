(ns remongo.utils
  "Utilities for making Remongo work and interact with easier"
  (:require [clojure.walk :as walk]))

(def ID-KEYS [:_id :id "_id" "id"])

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

(defn- make-false
  "Make anything falsey into false"
  [x]
  (or x false))


(defn id-to-str
  "Create string from an ID object (or string)"
  [id]
  (str id))

(defn id-to-obj
  "Create ObjectID from ID object (or string), if possible"
  [id]
  (if (and (string? id) (re-matches #"^[A-Fa-f\d]{24}$" id))
    {"$oid" id}
    id))

(defn get-id
  "Get ID of the document, if any, as a string or object (if ensure-obj is true)"
  [doc & {:keys [ensure-obj] :or {ensure-obj false}}]
  (when-let [id-obj (some (partial get doc) ID-KEYS)]
    (let [id-obj' ((if ensure-obj id-to-obj id-to-str) id-obj)]
      id-obj')))

(defn remove-id
  "Remove ID from the document"
  [doc]
  (apply dissoc doc ID-KEYS))

(defn normalize-id
  "Normalize id of given document, which is either to a string or object (if ensure-obj is true)"
  [doc & {:keys [ensure-obj] :or {ensure-obj false}}]
  (if-let [[k v] (some #(when-let [v (get doc %)] [% v]) ID-KEYS)]
    (assoc doc k ((if ensure-obj id-to-obj id-to-str) v))
    doc))

(defn objectify-condition
  "Objectify IDs mentioned in condition"
  [condition]
  (normalize-id condition :ensure-obj true))

(defn objectify-fields
  "Objectify IDs mentioned in fields, such as in projections"
  [fields]
  (if (:projections fields)
    (update fields :projections #(normalize-id % :ensure-obj true))
    fields))


