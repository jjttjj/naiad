(ns naiad.passes.non-channel-edges-to-nodes
  (:require [naiad.graph :as g])
  (:import (java.util IdentityHashMap)))

(defprotocol IToEdge
  (insert-into-graph [this graph] "Insert this non-channel edge as a new node in the graph"))


(extend-protocol IToEdge
  clojure.lang.PersistentVector
  (insert-into-graph [this graph]
    (let [id (g/gen-id)]
      {:id    id
       :graph (g/add-node graph {:type    :naiad/onto-chan
                                 :id      id
                                 :coll    this
                                 :outputs {:out id}})}))


  clojure.core.async.impl.protocols.ReadPort
  (insert-into-graph [this graph]
    (let [id (g/gen-id)]
      {:id    id
       :graph (g/annotate-link graph id {:existing-channel this})}))

  clojure.core.async.impl.protocols.WritePort
  (insert-into-graph [this graph]
    (let [id (g/gen-id)]
      {:id    id
       :graph (g/annotate-link graph id {:existing-channel this})}))

  clojure.lang.ISeq
  (insert-into-graph [this graph]
    (let [id (g/gen-id)]
      {:id    id
       :graph (g/add-node graph {:type    :naiad/onto-chan
                                 :id      id
                                 :coll    this
                                 :outputs {:out id}})})))


(defn non-channel-edges-to-nodes [graph]
  (let [seen (IdentityHashMap.)
        update-fn
        (fn [graph port-key]
          (reduce
            (fn [graph [node-id node]]
              (reduce
                (fn [graph [port-id port]]
                  (if (g/id? port)
                    graph
                    (let [{:keys [id graph]} (if (.containsKey seen port)
                                               {:id    (.get seen port)
                                                :graph graph}
                                               (insert-into-graph port graph))]
                      (.put seen port id)
                      (assoc-in graph [node-id port-key port-id] id))))
                graph
                (port-key node)))
            graph
            graph))]
    (reduce update-fn graph [:inputs :outputs :closes])))