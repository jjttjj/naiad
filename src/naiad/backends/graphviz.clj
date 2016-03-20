(ns naiad.backends.graphviz
  (:require [naiad.graph :as graph]
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
          (let [without-meta (dissoc node :inputs :outputs :closes :id :type)]
            (print (pr-str (str id)) "[label= ")

            (print (pr-str (apply str (:type node) \newline
                             (interpose \newline (map
                                                   (fn [[k v]]
                                                     (str k " = " v))
                                                   without-meta)))))

            (println "]")))
        (println "}"))))

  )
