(ns naiad.passes
  (:require [naiad.passes.remove-distributes :refer [remove-distributes]]
            [naiad.passes.fuse-transducer-nodes :refer [fuse-transducers]]
            [naiad.passes.insert-duplicators :refer [insert-duplicators]]
            [naiad.passes.non-channel-edges-to-nodes :refer [non-channel-edges-to-nodes]]))


(defn run-passes [graph]
  (-> graph
    non-channel-edges-to-nodes
    insert-duplicators
    fuse-transducers
    remove-distributes))
