(ns remongo.collection
  "Interacts with MongoDB collections"
  (:require [cljs.core.async :as async :refer [go]]
            [cljs.core.async.interop :refer [<p!]]
            [taoensso.timbre :as timbre]
            [remongo.utils :as utils]))


;; Neeed to turn off inference temporarily, due to the 'mongoClient' not being found
(set! *warn-on-infer* false)

(def REALM-MONGO-CLIENT (atom nil))

(defn make-collection
  "Get the MongoDB collection for the given name"
  [^js/String db-name ^js/String coll-name]
  (-> @REALM-MONGO-CLIENT
      (.db db-name)
      (.collection coll-name)))

(defn <findOne
  "Find one MongoDB document, returning a channel to the result, with false being the value if not found"
  [collection condition & {:keys [fields] :or {fields {}}}]
  ;; NOTE: we need to handle falsey values, and make sure it is indeed false, since
  ;; we can't put nil onto channels
  (go
    (let [coll-name (.-name collection)
          res (some->
                collection
                (.findOne (clj->js (or (some-> condition utils/objectify-condition) {}))
                          (clj->js (or (some-> fields utils/objectify-fields) {})))
                (<p!)
                js->clj)
          _ (timbre/debug "<findOne for res for collection" coll-name "and condition" condition
                          ": " res)
          norm-res (utils/normalize-id res)]
      (timbre/debug "<findOne got norm-res" norm-res)
      (utils/make-false norm-res))))

(defn <find
  "Find many MongoDB documents for given collection name and condition, returning a channel holding a vector"
  [collection condition & {:keys [fields] :or {fields {}}}]
  (go (->> (.find collection
                  (clj->js (or (some-> condition utils/objectify-condition) {}))
                  (clj->js (or (some-> fields utils/objectify-fields) {})))
           (<p!)
           js->clj
           (map utils/normalize-id)
           vec
           utils/make-false)))

(defn <insertMany
  "Insert many documents into a MongoDB collection, returning a channel of result"
  [collection docs]
  (go
    ;; Realm Web SDK doesn't deal with insertion of empty collections
    (if (empty? docs)
      (do
        (timbre/warn "Trying to insert empty sequence into collection" (.-name collection))
        true)
      (-> collection (.insertMany (clj->js docs)) (<p!) js->clj))))

(defn <delete
  "Delete one specific document"
  [collection doc]
  (go (-> collection (.deleteOne (clj->js {"_id" (utils/get-id doc :ensure-obj true)})) (<p!) (js->clj))))

(defn <deleteSeq
  "Helper to delete a sequence of documents, returning sequence of results"
  [collection docs]
  (go
    (let [ch (async/map identity (map (partial <delete collection) docs))
          coll (async/take (count docs) ch)]
      coll)))

(defn <deleteMany
  "Delete many documents from a MongoDB collection, given a condition, returning result in channel"
  [collection condition]
  (go (-> collection (.deleteMany (clj->js (utils/objectify-condition (some-> condition utils/objectify-condition)))) (<p!) js->clj)))

(defn <updateOne
  "Update one document in a MongoDB collection, given condition and options, returning result in channel"
  [collection condition doc & {:keys [upsert] :or {upsert true}}]
  ;; NOTE: we remove the _id from the actual object, even if it did exist before
  ;; TODO: we should really only do this if the condition involves the ID
  (let [doc' (dissoc doc "_id")
        opts {:upsert upsert}
        props  {:$set (clj->js doc')}]
    (go (-> collection
            (.updateOne (clj->js (or (some-> condition utils/objectify-condition) {}))
                        (clj->js props)
                        (clj->js opts))
            (<p!) js->clj))))

(defn <updateSeq
  "Helper to update a sequence of documents"
  [collection docs & {:keys [upsert] :or {upsert true}}]
  (go
    (let [ch (async/map identity (map #(<updateOne collection
                                                   (when-let [id (utils/get-id % :ensure-obj true)] {:_id id}) % :upsert upsert) docs))
          coll (async/take (count docs) ch)]
      coll)))

