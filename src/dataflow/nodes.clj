(ns dataflow.nodes
  (:require [clojure.core :as clj]
            [dataflow.graph :refer [add-node! gen-id id? *graph* ports insert-into-graph IToEdge
                                    INode]]
            [dataflow.backends.csp :as csp]
            [clojure.core.async :refer [go <! >! close!] :as async]))


(defrecord Map [f inputs outputs]
  INode
  (transducer? [this]
    (= (count inputs) 1))
  (as-transducer [this]
    (clj/map f))
  (validate [this]
    (assert (= (set (keys inputs)) (set (range (count inputs))))
            "Inputs to map must be intengers in the range of 0+")
    (assert (ifn? f) "Function input to map must be a IFn")
    (assert (= (set (keys outputs))
               #{:out}) "The output of map must be named :out"))
  csp/ICSPNode
  (construct! [this]
    (go
      (let [out (:out outputs)]
        (loop [acc []]
          (if (< (count acc) (count inputs))
            (if-let [v (<! (inputs (count acc)))]
              (recur (conj acc v))
              (do (close! out)
                  (doseq [[k v] inputs]
                    (close! v))))
            (if (>! out (apply f acc))
              (recur [])
              (doseq [[_ v] inputs]
                (close! v)))))))))


(defn xf-pipe [to xf from]
  (go (let [a (volatile! [])
            rf (xf (fn [_ v]
                     (vswap! a conj v)))]
        (loop []
          (if-some [v (<! from)]
            (let [_ (rf nil v)
                  exit? (loop [[h & t] @a]
                          (when h
                            (if (>! to h)
                              (recur t)
                              :exit)))]
              (if exit?
                (close! to)
                (do (vreset! a [])
                    (recur))))

            (let [_ (xf nil)]
              (doseq [v @a]
                (>! to v))
              (close! to)))))))

(defrecord GenericTransducerNode [id xf inputs outputs]
  INode
  (transducer? [this]
    true)
  csp/ICSPNode
  (construct! [this]
    (xf-pipe (:out outputs) xf (:in inputs))))

(defmacro gen-transducer-node [f]
  `(fn ~f [& args#]
     (let [fargs# (butlast args#)
           input# (last args#)
           output# (gen-id)]
       (add-node! (->GenericTransducerNode (gen-id)
                                           (apply ~f fargs#)
                                           {:in input#}
                                           {:out output#}))
       output#)))



(defn map-keys [mp from to]
  (println mp from to)
  (if-let [v (get mp from)]
    (assoc-in (dissoc mp from) to v)
    mp))




(defrecord PromiseAccumulator [id p inputs]
  csp/ICSPNode
  (construct! [this]
    (async/take! (async/into [] (:in inputs))
                 (partial deliver p))))



(defrecord Duplicate [id inputs outputs]
  INode
  (transducer? [this] false)
  (validate [this]
    (assert (= (keys inputs) [:in]) "Duplicate takes only one input")
    (assert (> (count outputs) 0) "Must have at least one output"))

  csp/ICSPNode
  (construct! [this]
    (let [in (:in inputs)
          outs (vec (vals outputs))]
      (go (loop []
            (if-some [v (<! in)]
              (let [exit? (loop [remain (set (map #(vector % v) outs))]
                            (when (> (count remain) 0)
                              (let [[ret c] (async/alts! (vec remain))]
                                (if ret
                                  (recur (disj remain [c v]))
                                  (do (doseq [c outs]
                                        (close! c))
                                      (close! in)
                                      :exit)))))]
                (when-not exit?
                  (recur)))
              (doseq [c outs]
                (close! c))))))))

