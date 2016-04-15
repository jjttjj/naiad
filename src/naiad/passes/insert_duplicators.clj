(ns naiad.passes.insert-duplicators
  (:require [naiad.graph :as g]))

(defn insert-duplicators [graph]
  (let [links (->> (g/ports graph :inputs)
                (group-by :link)
                (filter (fn [[k v]]
                          (> (count v) 1))))
        new-nodes (for [[link inputs] links
                        :let [new-ids (repeatedly (count inputs) g/gen-id)]]
                    {:node  {:type    :naiad/duplicate
                             :id      (g/gen-id)
                             :inputs  {:in link}
                             :outputs (zipmap (range) new-ids)}
                     :links (zipmap new-ids
                              inputs)})
        graph (reduce
                (fn [acc {:keys [node links]}]
                  (let [acc (assoc acc (:id node) node)]
                    (reduce
                      (fn [acc [new-id {:keys [node port link]}]]
                        (assoc-in acc [node :inputs port] new-id))
                      acc
                      links)))
                graph
                new-nodes)]
    graph))
