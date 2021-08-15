(ns remongo.utils
  "Utilities for making Remongo work and interact with easier")

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

