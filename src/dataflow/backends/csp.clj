(ns dataflow.backends.csp
  (:require [clojure.core.async :as async]
            [dataflow.graph :refer [links]]))


(defprotocol ICSPNode
  (construct! [this]))


(defn inject-channels [mappings mp]
  (reduce-kv
    (fn [acc k v]
      (let [c (mappings v)]
        (assert c (str "Mapping not found for " v))
        (assoc acc k c)))
    mp
    mp))


(defn construct-graph [graph]
  (let [link-ids (links graph)
        chans (zipmap link-ids (repeatedly async/chan))]
    (reduce-kv
      (fn [acc k node]
        (let [{:keys [inputs outputs closes]} node
              new-node (assoc node
                         :inputs (inject-channels chans inputs)
                         :outputs (inject-channels chans outputs)
                         :closes (inject-channels chans closes))]
          (construct! new-node)
          (assoc acc k new-node)))
      graph
      graph)))
