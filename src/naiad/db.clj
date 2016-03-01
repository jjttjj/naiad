(ns naiad.db
  (:require [clojure.set :as set]))

;; The original prototype of this library just stored the database in a hashmap of nodes,
;; but that got both complex and slow fairly quickly. So this namespace now defines a fairly fast
;; and efficient graph database

(defrecord Link [node port class link])
(defrecord Node [id structure])


(defrecord DB [nodes links])

(def conj-set (fnil conj #{}))

(defn add-link [db link]
  (conj db link))

(defn add-node [db {:keys [id] :as node}]
  {:pre [id]}
  (let [db (reduce
             (fn [db klass]
               (reduce-kv
                 (fn [db k v]
                   (add-link db (->Link id k klass v)))
                 db
                 (klass node)))
             db
             #{:inputs :outputs :closes})]
    (conj db (->Node id node))))


(defn query [{:keys [links]} & {:as kvs}]
  (filter
    (fn [link]
      (reduce-kv
        (fn [_ k v]
          (or
            (= (get link k) v)
            (reduced false)))
        true
        kvs))
    links))

(defn combine-db [& dbs]
  (apply set/union dbs))

(-> #{}
  (add-node {:id 42 :inputs {4 3}}))


(defmacro FlowVar [link-id db])

(defn merge-flow-vars [& vars]
  (let [merged-db (apply set/union (map :db vars))]
    (mapv #(assoc % :db merged-db))))