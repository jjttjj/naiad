(ns naiad
  (:refer-clojure :exclude [map take filter partition-all merge])
  (:require [clojure.core :as clj]
            [clojure.core.async :as async]
            [naiad.graph :refer [add-node! add-node gen-id id? *graph* ports
                                 INode ->GraphSource annotate-link add-port! annotate-link!]
                         :as graph]
            [naiad.backends.csp :as csp]
            [naiad.nodes :refer :all]
            [naiad.graph :as graph]
            [naiad.metrics]
            [naiad.passes :as passes]))

(def ^:dynamic *io* false)

(defmacro with-annotations [ann & body]
  `(binding [graph/*default-annotations* ~ann]
     ~@body))



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








(defmacro flow
  "Creates a graph and executes it using standard optimizations."
  [& body]
  `(binding [*graph* {}]
     ~@body
     (csp/construct-graph (passes/run-passes *graph*))))

(defmacro graph [& body]
  `(binding [*graph* {}]
     ~@body
     (passes/run-passes *graph*)))

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

(defn no-close [in]
  (let [out (gen-id)]
    (add-node!
      {:type ::no-close
       :inputs {:in in}
       :outputs {:out out}})
    out))

(defn multiplexer [selector inputs]
  (let [out (gen-id)]
    (add-node!
      {:type ::multiplexer
       :input-count (count inputs)
       :inputs (assoc (zipmap (range) inputs)
                 :selector selector)
       :outputs {:out out}})
    out))

(defn demultiplexer [num-outputs input]
  (let [selector (gen-id)
        outputs (repeatedly num-outputs gen-id)]
    (add-node!
      {:type         ::demultiplexer
       :output-count num-outputs
       :inputs       {:in input}
       :outputs      (assoc (zipmap (range) outputs)
                       :selector selector)})
    {:selector selector
     :outputs outputs}))

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


(defn subscribe [topic-fn in]
  (let [node-id (gen-id)]
    (add-node! {:type ::subscribe
               :topic-fn topic-fn
               :id node-id
               :inputs {:in in}
               :outputs {}})
    (reify
      clojure.lang.ILookup
      (valAt [this val]
        (add-port! node-id :outputs val))
      (valAt [this val _]
        (.valAt this val)))))




(defn parallel* [{:keys [threads ordered?]} f in]
  (if ordered?
    (let [{:keys [selector outputs]} (demultiplexer threads in)]
      (multiplexer selector (clj/map f outputs)))

    (let [outs (mapv f
                 (clj/take threads (distribute in)))]
      (apply merge outs))))

(defmacro parallel->> [opts & body]
  `(parallel* ~opts
     (fn [in#]
       (->> in# ~@(butlast body)))
     ~(last body)))

(defmacro flow-result [& expr]
  `(let [p# (promise)]
     (flow
       (promise-accumulator p# (do ~@expr)))
     @p#))


#_(naiad.backends.graphviz/output-dotfile (let [p (promise)]
                                            (graph
                                              (let [v      (take 3 [1 2 3 4])
                                                    result (map vector (map inc v)
                                                             (map dec v))]
                                                (promise-accumulator p result))))
    "test.dot")


(defn annotate
  ([link k v & rest]
    (annotate link (apply hash-map k v rest)))
  ([link meta]
   (annotate-link! link meta)
   link))

(defmacro blocking-io [& body]
  (binding [*io* true]
    ~@body))



(comment

  (->> foo
    (map inc)
    (parallel-> {:threads 3}
      (map dec)
      (filter pos?))
    (take 3)
    )

  )