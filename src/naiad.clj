(ns naiad
  (:refer-clojure :exclude [map take filter partition-all merge])
  (:require [clojure.core :as clj]
            [clojure.core.async :as async]
            [naiad.graph :refer [add-node! add-node gen-id id? *graph* ports insert-into-graph IToEdge
                                 INode ->GraphSource]]
            [naiad.backends.csp :as csp]
            [naiad.nodes :refer :all]
            [naiad.graph :as graph])
  (:import (java.util IdentityHashMap)))




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
                    {:node  {:type    ::duplicate
                             :id      (gen-id)
                             :inputs  {:in link}
                             :outputs (zipmap (range) new-ids)}
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

(defmethod csp/construct! ::onto-chan
  [{:keys [outputs coll]}]
  (clojure.core.async/onto-chan (:out outputs) coll true))

(extend-protocol IToEdge
  clojure.lang.PersistentVector
  (insert-into-graph [this graph]
    (let [id (gen-id)]
      {:id    id
       :graph (add-node graph {:type ::onto-chan
                               :id id
                               :coll this
                               :outputs {:out id}})}))

  clojure.lang.ISeq
  (insert-into-graph [this graph]
    (let [id (gen-id)]
      {:id    id
       :graph (add-node graph {:type ::onto-chan
                               :id id
                               :coll this
                               :outputs {:out id}})})))


(defn non-channel-edges-to-nodes [graph]
  (let [seen (IdentityHashMap.)]
    (reduce
      (fn [graph [node-id {:keys [inputs] :as node}]]
        (reduce
          (fn [graph [input-id input]]
            (if (id? input)
              graph
              (let [{:keys [id graph]} (if (.containsKey seen input)
                                         {:id    (.get seen input)
                                          :graph graph}
                                         (insert-into-graph input graph))]
                (.put seen input id)
                (assoc-in graph [node-id :inputs input-id] id))))
          graph
          inputs))
      graph
      graph)))

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

(defn remove-distributes [graph]
  (let [id-maps (atom {})
        graph   (reduce-kv
                  (fn [acc id {:keys [type] :as node}]
                    (if (= ::distribute type)
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
      (->> (graph/ports graph :inputs)
        (clj/filter (comp @id-maps :link))))))

(defmacro flow
  "Creates a graph and executes it using standard optimizations."
  [& body]
  `(binding [*graph* {}]
     ~@body
     (csp/construct-graph (remove-distributes (fuse-transducers (insert-duplicators (non-channel-edges-to-nodes *graph*)))))))

(defmacro graph [& body]
  `(binding [*graph* {}]
     ~@body
     (remove-distributes (fuse-transducers (insert-duplicators (non-channel-edges-to-nodes *graph*))))))

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
     (assoc :type ::map)
     add-node!))
  ([h & r]
   (let [opts (apply hash-map h r)]
     (->map opts))))

(defn map
  "Constructs a mapping node that applies a function to values taken from one or more channels.
  Multiple inputs are supported. Values are applied to f in lockstep, so that (map f a b) will first
  call f with [a0 b0] then with [a1 b1], etc. "
  [f & ins]
  (let [out (gen-id)]
    (->map
      :id (gen-id)
      :f f
      :out out
      :inputs (zipmap (range) ins))
    out))


(defn merge [& ins]
  (let [out (gen-id)]
    (add-node!
      {:type ::merge
       :inputs (zipmap (range) ins)
       :outputs{:out out}})
    out))




(defn promise-accumulator [p input]
  (add-node! {:type    ::promise-accumulator
              :promise p
              :inputs  {:in input}})
  nil)


(def ^{:doc "Creates a node that will take the first X items from the input port before closing both the
             input and output ports.

             Usage:
             (take n input)
             n     - The number of items to take from input
             input - Input node to take n items from

             Returns:
             A link that will receive n items taken from input"}
  take
  (gen-transducer-node clj/take))

(def ^{:doc "Creates a node that will take the first X items from the input port before closing both the
             input and output ports.

             Usage:
             (take :n x :in input :out output)
             n      - (required) The number of items to take from in
             input  - (required) Input node to take n items from
             output - output node to put items taken form in

             Returns:
             :out, will construct a link if one is not given"}
  ->take
  (gen-verbose-transducer-node clojure.core/take [:n]))

(def filter (gen-transducer-node clj/filter))
(def partition-all (gen-transducer-node clj/partition-all))

(defn distribute [in]
  (let [node-id (gen-id)]
    (add-node! {:type ::distribute
                :id node-id
                :inputs {:in in}
                :outputs {}})
    (clj/map
      (fn [idx]
        (let [id (gen-id)]
          (set! *graph* (assoc-in *graph* [node-id :outputs idx] id))
          id))
      (range))))




(defn parallel* [{:keys [threads]} f in]
  (let [outs (mapv f
               (clj/take threads (distribute in)))]
    (apply merge outs)))

(defmacro parallel->> [opts & body]
  `(parallel* ~opts
     (fn [in#]
       (->> in# ~@(butlast body)))
     ~(last body)))

#_(naiad.backends.graphviz/output-dotfile (let [p (promise)]
                                            (graph
                                              (let [v      (take 3 [1 2 3 4])
                                                    result (map vector (map inc v)
                                                             (map dec v))]
                                                (promise-accumulator p result))))
    "test.dot")


(comment

  (->> foo
    (map inc)
    (parallel-> {:threads 3}
      (map dec)
      (filter pos?))
    (take 3)
    )

  )