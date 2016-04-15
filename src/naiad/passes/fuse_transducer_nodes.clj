(ns naiad.passes.fuse-transducer-nodes
  (:require [naiad.graph :as graph]))

(defn fuse-transducer-nodes [graph from to]
  (let [f1       (get-in graph [from :f])
        f2       (get-in graph [to :f])
        new-f    (comp f1 f2)
        _        (assert f1)
        _        (assert f2)
        new-node {:type        ::fused-transducer
                  :id          from
                  :transducer? true
                  :f           new-f
                  :inputs      (:inputs (graph from))
                  :outputs     (:outputs (graph to))}]
    (-> graph
      (dissoc to)
      (assoc from new-node))))

(defn fuse-transducers [graph]
  (let [xducer (->> (for [[id node] graph
                          :when (and (:transducer? node)
                                  (= 1 (count (:outputs node))))
                          :let [onodes (graph/nodes-by-link graph :inputs (fnext (first (:outputs node))))]
                          :when (= (count onodes) 1)
                          :let [onode (graph (:node (first onodes)))]
                          :when (:transducer? onode)
                          ]
                      {:from node
                       :to   onode
                       })
                 first)]
    (if xducer
      (recur (fuse-transducer-nodes graph (:id (:from xducer)) (:id (:to xducer))))
      graph)))
