(ns remongo.mongo
  "Wrapper and utilities for dealing with MongoDB, including in the context of Reframe"
  (:require
    [cljs.core.async :as async :refer [go <!]]
    [cljs.core.async.interop :refer-macros [<p!]]
    [taoensso.timbre :as timbre]
    ["realm-web" :as realm]
    [remongo.utils :as utils :refer [dissoc-in]]))

(def REALM-APP (atom nil))
(def REALM-CRED (atom nil))
(def REALM-MONGO-CLIENT (atom nil))


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
  "Use a sync layer to save extracts from Reframe DB to MongoDB, returning the updated Reframd DB"
  [sync-info db-chan layer]
  (go
    (let [db (<! db-chan)
          collection (:collection layer)
          db-name (or (:db layer) (:db sync-info))]
      (timbre/info "<sync-save-layer with layer" layer)
      (case (:kind layer)
        :single (let [_res (<! (<updateOne db-name collection (:keys layer) db))]
                  (dissoc-in db (:path layer)))
        :many (let [path-value (get-in db (:path layer))
                    path-key (:path-key layer)
                    items (if path-key (vals path-value) path-value)
                    _ (timbre/info "Got" (count items) "items for collection" collection)
                    ;; TODO: we are currently always deleting and reinserting these documents
                    del-result (<! (<deleteMany db-name collection {}))
                    _ (timbre/info "Deleting from DB" db-name "and collection" collection
                                   "with result:" (js->clj del-result))
                    ins-result (<! (<insertMany db-name collection items))
                    _ (timbre/info "Inserting into DB" db-name "and collection" collection
                                   "with result:" (js->clj ins-result))]
                (timbre/info "Trying to dissoc" (:path layer) "from DB")
                (dissoc-in db (:path layer)))
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
  [sync-info db-chan layer]
  (go
    (let [db (<! db-chan)
          collection (:collection layer)
          db-name (or (:db layer) (:db sync-info))
          path (make-string (:path layer))
          path-key (make-string (:path-key layer))
          _ (timbre/info "<sync-load-layer about to load layer" layer "with path" path)
          db'
          (case (:kind layer)
            :single (let [obj (<! (<findOne db-name collection (:keys layer)))
                          db' (cond
                                (false? obj) (do (timbre/warn "<sync-load-layer could not find any" collection)
                                                 db)
                                ;; If we have an empty path, we replace the whole Reframe DB
                                (empty? path) obj
                                :else (assoc-in db path obj))]
                      db')
            :many (let [items (<! (<find db-name collection {}))
                        _ (timbre/info "<sync-load-layer got" (count items) "items from collection" collection
                                       "using path key" path-key "and path" path)
                        ;; We have to fit the items into the Reframe DB, either as is, or as a map
                        path-value (if path-key (into {} (map (fn [item] [(get item path-key) item]) items))
                                                items)
                        _ (timbre/info "Keys are " (when (map? path-value) (keys path-value)))
                        db' (assoc-in db path path-value)]
                    db')
            (do (timbre/error "Trying to load with invalid sync layer:" layer) db))]
      db')))

(defn <sync-save
  "Save a Reframe DB to MongoDB intelligently, returning the fitered (often empty) Reframe DB"
  [db opts]
  (let [ch (async/chan)
        layers (:layers opts)]
    (async/put! ch db)
    (reduce (partial <sync-save-layer opts) ch (reverse layers))))

(defn <sync-load
  "Load a Reframe DB from MongoDB intelligently, returning the actual Reframe DB"
  [opts]
  (let [ch (async/chan)
        layers (:layers opts)]
    (timbre/info "<sync-load with layers" layers)
    (async/put! ch {})
    (reduce (partial <sync-load-layer opts) ch layers)))
