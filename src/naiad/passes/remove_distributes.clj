(ns naiad.passes.remove-distributes
  (:require [naiad.graph :as g]))

(defn remove-distributes [graph]
  (let [id-maps (atom {})
        graph (reduce-kv
                (fn [acc id {:keys [type] :as node}]
                  (if (= :naiad/distribute type)
                    (do (apply swap! id-maps assoc (interleave
                                                     (vals (:outputs node))
                                                     (repeat (:in (:inputs node)))))
                        acc)
                    (assoc acc id node)))
                {}
                graph)]
    (reduce
      (fn [graph {:keys [node link port]}]
        (assoc-in graph [node :inputs port] (@id-maps link)))
      graph
      (->> (g/ports graph :inputs)
        (filter (comp @id-maps :link))))))