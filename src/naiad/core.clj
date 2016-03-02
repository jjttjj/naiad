(ns naiad.core
  (:refer-clojure :exclude [map take filter partition-all])
  (:require [clojure.core :as clj]
            [clojure.core.async :as async]
            [naiad.graph :refer [add-node! gen-id id? *graph* ports insert-into-graph IToEdge
                                 INode ->GraphSource]]
            [naiad.backends.csp :as csp]
            [naiad.nodes :refer :all]
            [naiad.graph :as graph])
  (:import (naiad.nodes GenericTransducerNode)))




(comment (defn destructurable [id]
           (reify
             clojure.lang.ILookup
             (valAt [this val]
               (let [result (gen-id)]
                 (set! *graph* (assoc-in *graph* [id :outputs val] result))
                 result))))

  (defrecord Duplicate [id inputs outputs]
    INode
    (transducer? [this] false)
    (validate [this]
      (assert (= (keys inputs) [:in]) "Duplicate takes only one input")
      (assert (> (count outputs) 0) "Must have at least one output")))

  (defn splitter [pred input]
    (let [id  (gen-id)
          out (destructurable id)]
      (add-node! {:op     ::split
                  :pred   pred
                  :id     id
                  :inputs {:in input}})
      out))

  )


(defn insert-duplicators [graph]
  (let [links     (->> (ports graph :inputs)
                    (group-by :link)
                    (clj/filter (fn [[k v]]
                                  (> (count v) 1))))
        new-nodes (for [[link inputs] links
                        :let [new-ids (repeatedly (count inputs) gen-id)]]
                    {:node  (map->Duplicate
                              {:id      (gen-id)
                               :inputs  {:in link}
                               :outputs (zipmap (range) new-ids)})
                     :links (zipmap new-ids
                              inputs)})
        graph     (reduce
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

(defrecord OntoChan [id coll outputs]
  INode
  (transducer? [this] false)
  (validate [this])
  csp/ICSPNode
  (construct! [this]
    (clojure.core.async/onto-chan (:out outputs) coll true))
  )

(extend-protocol IToEdge
  clojure.lang.PersistentVector
  (insert-into-graph [this graph]
    (let [id (gen-id)]
      {:id    id
       :graph (assoc graph id (->OntoChan id this {:out id}))}))

  #_clojure.core.async.impl.protocols/ReadPort
  #_(insert-into-graph [this graph]
      (let [id (gen-id)]
        {:id    id
         :graph (assoc graph id (->Pipe))})))


(defn non-channel-edges-to-nodes [graph]
  (reduce
    (fn [graph [node-id {:keys [inputs] :as node}]]
      (reduce
        (fn [graph [input-id input]]
          (if (id? input)
            graph
            (let [{:keys [id graph]} (insert-into-graph input graph)]
              (assoc-in graph [node-id :inputs input-id] id))))
        graph
        inputs))
    graph
    graph))

(defn fuse-transducer-nodes [graph from to]
  (let [f1 (get-in graph [from :xf])
        f2 (get-in graph [to :xf])
        new-f (comp f1 f2)
        _ (assert f1)
        _ (assert f2)
        new-node (->GenericTransducerNode from
                   new-f
                   (:inputs (graph from))
                   (:outputs (graph to)))]
    (-> graph
      (dissoc to)
      (assoc from new-node))))

(defn fuse-transducers [graph]
  (let [xducer (->> (for [[id node] graph
                          :when (and (instance? GenericTransducerNode node)
                                  (= 1 (count (:outputs node))))
                          :let [onodes (graph/nodes-by-link graph :inputs (fnext (first (:outputs node))))]
                          :when (= (count onodes) 1)
                          :let [onode (graph (:node (first onodes)))]
                          :when (instance? GenericTransducerNode onode)
                          ]
                      {:from node
                       :to   onode
                       })
                 first)]
    (if xducer

      (recur (fuse-transducer-nodes graph (:id (:from xducer)) (:id (:to xducer))))
      graph)))

(defmacro flow [& body]
  `(binding [*graph* {}]
     ~@body
     (csp/construct-graph (fuse-transducers (insert-duplicators (non-channel-edges-to-nodes *graph*))))))

(defmacro graph [& body]
  `(binding [*graph* {}]
     ~@body
     (fuse-transducers (insert-duplicators (non-channel-edges-to-nodes *graph*)))))

(comment (let [p (promise)]
           (flow
             (nodes/promise-accumulator p (nodes/map inc [1 2 3 4])))
           @p)

  (let [p (promise)]
    (binding [*graph* {}]

      (nodes/promise-accumulator p (nodes/map inc [1 2 3 4]))
      *graph*
      (csp/construct-graph (non-channel-edges-to-nodes *graph*)))
    @p))



;; Node Constructors

(defn ->map
  ([mp]
   (-> mp
     (map-keys :in [:inputs 0])
     (map-keys :out [:outputs :out])
     map->Map))
  ([h & r]
   (let [opts (apply hash-map h r)]
     (->map opts))))

(defn map
  "Constructs a mapping node that applies a function to values taken from one or more channels.
  Multiple inputs are supported. Values are applied to f in lockstep, so that (map f a b) will first
  call f with [a0 b0] then with [a1 b1], etc. "
  [f & ins]
  (let [out (gen-id)]
    (add-node!
      (->map
        :id (gen-id)
        :f f
        :out out
        :inputs (zipmap (range) ins)))
    out))



(defn promise-accumulator [p input]
  (let [id (gen-id)]
    (add-node! (->PromiseAccumulator id p {:in input}))
    id))


(def take (gen-transducer-node clj/take))
(def filter (gen-transducer-node clj/filter))
(def partition-all (gen-transducer-node clj/partition-all))


#_(naiad.backends.graphviz/output-dotfile (let [p (promise)]
                                            (graph
                                              (let [v      (take 3 [1 2 3 4])
                                                    result (map vector (map inc v)
                                                             (map dec v))]
                                                (promise-accumulator p result))))
    "test.dot")