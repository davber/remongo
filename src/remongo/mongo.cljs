(ns remongo.mongo
  "Wrapper and utilities for dealing with MongoDB, including in the context of Reframe"
  (:require
    [clojure.set :as set]
    [cljs.core.async :as async :refer [go <!]]
    [cljs.core.async.interop :refer-macros [<p!]]
    [taoensso.timbre :as timbre]
    ["realm-web" :as realm]
    [remongo.utils :as utils :refer [dissoc-in]]))

(def REALM-APP (atom nil))
(def REALM-CRED (atom nil))
(def REALM-MONGO-CLIENT (atom nil))

;; The sync cache will map sync structure + layer index to a cache
(def REALM-SYNC-CACHE (atom nil))

(defn clear-cache
  "Clear all layer caches for given sync info"
  [sync-info]
  (swap! REALM-SYNC-CACHE dissoc sync-info))

(defn update-layer-cache
  "Update sync cache specific for the given layer"
  [sync-info layer-index data]
  (swap! REALM-SYNC-CACHE assoc-in [sync-info layer-index] data))

(defn get-layer-cache
  "Get the layer cache"
  [sync-info layer-index]
  (get-in @REALM-SYNC-CACHE [sync-info layer-index]))

(defn extract-layer-diff
  "Get the difference from the existing cache for a layer, as a three keyed structure, :insert, :update and :delete"
  [sync-info layer-index data-clj]
  ;; We do handle documents without ID:s, by assuming they are new
  ;; TODO: return the new documents with the proper ID's filled in, along with the reduced Reframe DB
  (let [data (utils/stringify data-clj)
        id-docs (filter #(get % "_id") data)
        no-id-docs (remove #(get % "_id") data)
        old-data (get-layer-cache sync-info layer-index)
        old-ids (set (map #(get % "_id") old-data))
        current-ids (set (map #(get % "_id") id-docs))
        inserted-ids (set/difference current-ids old-ids)
        deleted-ids (set/difference old-ids current-ids)
        surviving-ids (set/intersection old-ids current-ids)
        inserted-docs (filter (comp inserted-ids #(get % "_id")) data)
        ;; And we add all documents that don't already have an ID
        inserted-docs' (concat inserted-docs no-id-docs)
        deleted-docs (filter (comp deleted-ids #(get % "_id")) data)
        ;; We need to check which of the surviving documents that really changed
        new-docs (filter (comp surviving-ids #(get % "_id")) data)
        old-docs (filter (comp surviving-ids #(get % "_id")) old-data)
        old-map (into {} (map (juxt #(get % "_id") identity) old-docs))
        updated-docs (remove (fn [doc] (= doc (old-map (#(get % "_id") doc)))) new-docs)
        diff {:insert inserted-docs' :delete deleted-docs :update updated-docs}]
    (timbre/info "old-ids = \n" old-ids)
    (timbre/info "current-ids = \n" current-ids)
    (timbre/info "inserted-ids = \n" inserted-ids)
    (timbre/info "deleted-ids = \n" deleted-ids)
    (timbre/info "extract-layer-diff of layer" layer-index "with diff:\n" diff)
    diff))

(defn ^realm/User current-user
  "The currently logged in user, if any"
  []
  (.-currentUser ^realm/App @REALM-APP))

(defn <login
  "Login using default user, returning a channel returning either an ID or nil"
  []
  (timbre/info "About to get credentials")
  (let [^realm/App app @REALM-APP
        cred (.anonymous realm/Credentials)
        cred' (utils/string-object cred)]
    (timbre/info "Got credentials:" cred')
    (reset! REALM-CRED cred)
    (timbre/info "About to login")
    (go
      (try
        (let [user (<p! (.logIn app cred))]
          (timbre/info "Logged in with user " user " and ID: " (.-id (current-user)))
          user)
        (catch js/Error err
          (timbre/error "Login error: " err))))))

(defn init-app
  "Initialize the Realm app"
  [app-id]
  ;; Create a Realm app proxy
  (timbre/info "About to create Realm app")
  (let [^realm/App app (realm/App. (clj->js {:id app-id}))
        app' (utils/string-object app)]
    (timbre/info "Created Realm app: " app')
    (reset! REALM-APP app)))

;; Neeed to turn off inference temporarily, due to the 'mongoClient' not being found
(set! *warn-on-infer* false)

(defn mongo-collection
  "Get the MongoDB collection for the given name"
  [^js/String db-name ^js/String coll-name]
  (-> @REALM-MONGO-CLIENT
      (.db db-name)
      (.collection coll-name)))

(defn- make-false
  "Make anything falsey into false"
  [x]
  (or x false))

(defn <findOne
  "Find one MongoDB document, returning a channel to the result, with false being the value if not found"
  [db-name coll-name condition]
  ;; NOTE: we need to handle falsey values, and make sure it is indeed false, since
  ;; we can't put nil onto channels
  (go (-> (mongo-collection db-name coll-name) (.findOne (clj->js condition)) (<p!) js->clj make-false)))

(defn <find
  "Find many MongoDB documents for given collection name and condition, returning a channel holding sequence"
  [db-name coll-name condition]
  (go (-> (mongo-collection db-name coll-name) (.find (clj->js condition)) (<p!) js->clj)))

(defn <insertMany
  "Insert many documents into a MongoDB collection, returning a channel of result"
  [db-name coll-name docs]
  (go
    (if (empty? docs)
      (do
        (timbre/warn "Trying to insert empty sequence into collection" coll-name)
        true)
      (-> (mongo-collection db-name coll-name) (.insertMany (clj->js docs)) (<p!) js->clj))))

(defn <delete
  "Delete one specific document"
  [db-name coll-name doc]
  (go (-> (mongo-collection db-name coll-name) (.delete (clj->js doc)) (<p!) (js->clj))))

(defn <deleteSeq
  "Helper to delete a sequence of documents, returning sequence of results"
  [db-name coll-name docs]
  (async/reduce cons nil (reverse (map (partial <delete db-name coll-name) docs))))

(defn <deleteMany
  "Delete many documents from a MongoDB collection, given a condition, returning result in channel"
  [db-name coll-name condition]
  (go (-> (mongo-collection db-name coll-name) (.deleteMany (clj->js condition)) (<p!) js->clj)))

(defn <updateOne
  "Update one document in a MongoDB collection, given condition and options, returning result in channel"
  [db-name coll-name condition doc & {:keys [upsert] :or {upsert true}}]
  (let [opts {:upsert upsert}
        props  {:$set (clj->js doc)}]
    (go (-> (mongo-collection db-name coll-name)
            (.updateOne (clj->js condition)
                        (clj->js props)
                        (clj->js opts))
            (<p!) js->clj))))

(defn <updateSeq
  "Helper to update a sequence of documents"
  [db-name coll-name docs & {:keys [upsert] :or {upsert true}}]
  (async/reduce cons nil (reverse (map #(<updateOne db-name coll-name {:_id (:_id %)} % :upsert upsert) docs))))

(defn <init
  "Initialize the Realm connection, returning a channel with either user or nil"
  [app-id app-variant]
  (init-app app-id)
  (go
    (let [^realm/User user (<! (<login))
          _ (timbre/info "Getting user from channel: " user " with ID " (when user (.-id user)))
          ^realm/MongoDB client (.mongoClient user app-variant)]
      (timbre/info "Got MongoDB client: " client)
      (reset! REALM-MONGO-CLIENT client)
      user)))

(defn <sync-save-layer
  "Use a sync layer to save extracts from Reframe DB to MongoDB, returning the updated Reframe DB"
  [sync-info db-chan layer-index]
  (go
    (let [layer (nth (:layers sync-info) layer-index)
          path (utils/stringify (:path layer))
          path-key (utils/stringify (:path-key layer))
          db (<! db-chan)
          collection (:collection layer)
          db-name (or (:db layer) (:db sync-info))]
      (timbre/info "<sync-save-layer with layer" layer "and path" path "and path-key" path-key)
      (case (:kind layer)
        ;; TODO: we should check the global for actual changes, using our sync cache
        :single (let [_res (<! (<updateOne db-name collection (:keys layer) db))]
                  (dissoc-in db path))
        :many (let [path-value (get-in db path)
                    path-key path-key
                    items (if path-key (vals path-value) path-value)
                    _ (timbre/info "Got" (count items) "items for collection" collection)
                    ;; We just do the proper deletes, insertions and updates
                    diff (extract-layer-diff sync-info layer-index items)
                    insert-docs (:insert diff)
                    ins-result (<! (<insertMany db-name collection insert-docs))
                    _ (timbre/info "Inserted" (count insert-docs) "into" collection "with result"
                                   (js->clj ins-result))
                    delete-docs (:delete diff)
                    del-results (<! (<deleteSeq db-name collection delete-docs))
                    _ (timbre/info "Deleting" (count delete-docs) "from DB" db-name "and collection" collection
                                   "with result:" (js->clj del-results))
                    update-docs (:update diff)
                    upd-results (<! (<updateSeq db-name collection items :upsert false))
                    _ (timbre/info "Updated" (count update-docs) "into DB" db-name "and collection" collection
                                   "with result:" (js->clj upd-results))]
                ;; TODO: updating cache to fit what we have right now
                (timbre/info "Trying to dissoc" path "from DB")
                (dissoc-in db path))
        (do (timbre/error "Trying to save with invalid sync layer:" layer) db)))))

(defn make-string
  "Turn a single keyword into a string and a sequence of keywords into strings"
  [k]
  (cond
    (seqable? k) (into (empty k) (map make-string k))
    (keyword? k) (name k)
    :else k))

(defn <sync-load-layer
  "Load data from MongoDB according to sync layer, adding on Reframe DB we get from channel"
  [sync-info db-chan layer-index]
  (go
    (let [layer (nth (:layers sync-info) layer-index)
          db (<! db-chan)
          collection (:collection layer)
          db-name (or (:db layer) (:db sync-info))
          path (utils/stringify (:path layer))
          path-key (utils/stringify (:path-key layer))
          _ (timbre/info "<sync-load-layer about to load layer" layer "with path" path)
          db'
          (case (:kind layer)
            :single (let [obj (<! (<findOne db-name collection (:keys layer)))
                          [db' cache]
                          (cond
                            (false? obj) (do (timbre/warn "<sync-load-layer could not find any" collection)
                                             [db []])
                            ;; If we have an empty path, we replace the whole Reframe DB
                            (empty? path) [obj [obj]]
                            :else [(assoc-in db path obj) [obj]])]
                      (update-layer-cache sync-info layer-index cache)
                      db')
            :many (let [items (<! (<find db-name collection {}))
                        _ (timbre/info "<sync-load-layer got" (count items) "items from collection" collection
                                       "using path key" path-key "and path" path)
                        ;; We have to fit the items into the Reframe DB, either as is, or as a map
                        path-value (if path-key (into {} (map (fn [item] [(get item path-key) item]) items))
                                                items)
                        _ (timbre/info "Keys are " (when (map? path-value) (keys path-value)))
                        db' (assoc-in db path path-value)]
                    (update-layer-cache sync-info layer-index items)
                    db')
            (do (timbre/error "Trying to load with invalid sync layer:" layer) db))]
      db')))

(defn <sync-save
  "Save a Reframe DB to MongoDB intelligently, returning the fitered (often empty) Reframe DB"
  [db opts]
  (let [ch (async/chan)
        layer-indices (range (count (:layers opts)))
        db' (utils/stringify db)]
    (async/put! ch db')
    (reduce (partial <sync-save-layer opts) ch (reverse layer-indices))))

(defn <sync-load
  "Load a Reframe DB from MongoDB intelligently, returning the actual Reframe DB"
  [opts]
  (let [ch (async/chan)
        layer-indices (range (count (:layers opts)))]
    (async/put! ch {})
    (reduce (partial <sync-load-layer opts) ch layer-indices)))
