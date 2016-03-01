(ns dataflow.backends.graphviz
  (:require [dataflow.graph :as graph]
            [clojure.java.io :refer [writer]]))

(defn output-dotfile [graph file]
  (let [edges (graph/edges graph)]
    (with-open [wtr (writer file)]
      (binding [*out* wtr]
        (println "digraph G {")
        (doseq [{:keys [link in out]} edges]
          (println (pr-str (str (:node in))) "->" (pr-str (str (:node out)))
                   "[ headlabel= " (pr-str (str (:port out)))
                   "taillabel=" (pr-str (str (:port in))) "];"))
        (doseq [[id node] graph]
          (println (pr-str (str id)) "[label= " (pr-str (str node)) "]"))
        (println "}"))))

  )
