(ns naiad.nodes
  (:require [clojure.core :as clj]
            [naiad.graph :refer [add-node! gen-id id? *graph* ports
                                 INode]]
            [naiad.backends.csp :as csp]
            [clojure.core.async :refer [go <! >! close! thread] :as async]))

(defmacro process [{:keys [blocking-io] :as opts} & body]
  (if blocking-io
    `(thread)))




(defmethod csp/construct! :naiad/map
  [{:keys [inputs outputs f]}]
  (go
    (let [out (:out outputs)]
      (loop [acc []]
        (if (< (count acc) (count inputs))
          (if-some [v (<! (inputs (count acc)))]
            (recur (conj acc v))
            (do (close! out)
                (doseq [[k v] inputs]
                  (close! v))))
          (if (>! out (apply f acc))
            (recur [])
            (doseq [[_ v] inputs]
              (close! v))))))))





(defmacro gen-transducer-node [f]
  `(fn ~(symbol (name f)) [& args#]
     (let [fargs#  (butlast args#)
           input#  (last args#)
           output# (gen-id)]
       (add-node!
         {:type        :generic-transducer
          :f           (apply ~f fargs#)
          :transducer? true
          :inputs      {:in input#}
          :outputs     {:out output#}})
       output#)))

(defmacro gen-verbose-transducer-node [f arg-names]
  (let [fname (symbol (str (name f) "-node"))]
    `(let [extractor# (apply juxt ~arg-names)]
       (fn ~fname
         ([first# & rargs#]
           (~fname (apply hash-map first# rargs#)))
         ([args#]
           (let [fargs# (extractor# args#)
                 input# (or (:in args#) (gen-id))
                 output# (or (:out args#) (gen-id))]
             (add-node!
               {:type        :generic-transducer
                :f           (apply ~f fargs#)
                :transducer? true
                :inputs      {:in input#}
                :outputs     {:out output#}})
             output#))))))



(defn map-keys [mp from to]
  (if-let [v (get mp from)]
    (assoc-in (dissoc mp from) to v)
    mp))





(defmethod csp/construct! :naiad/promise-accumulator
  [{:keys [inputs promise]}]
  (async/take! (async/into [] (:in inputs))
    (partial deliver promise)))


(defmethod csp/construct! :naiad/duplicate
  [{:keys [inputs outputs]}]
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
              (close! c)))))))

(defmethod csp/construct! :naiad/merge
  [{:keys [inputs outputs]}]
  (let [ins (vals inputs)
        out (:out outputs)]
    (go (loop [ins ins]
          (when (pos? (count ins))
            (let [[v c] (async/alts! ins)]
              (if v
                (if (>! out v)
                  (recur ins)
                  (do (doseq [in ins]
                        (close! ins))))
                (do (close! c)
                    (recur (remove (partial = c) ins)))))))
        (close! out))))


(defmethod csp/construct! :naiad/subscribe
  [{:keys [topic-fn inputs outputs default-key default-c]}]
  (let [default-c (or (and default-key (default-key outputs))
                    default-c)
        in-c (:in inputs)]
    (go
      (loop []
        (if-some [v (<! in-c)]
          (let [topic (topic-fn v)
                out-c (or (outputs topic)
                        default-c)]
            (if out-c
              (if (>! out-c v)
                (recur)
                (do (close! in-c)
                    (doseq [[k v] outputs]
                      (close! v))))
              (recur)))
          (do (doseq [[k v] outputs]
                (close! v))))))))




(defmethod csp/construct! :naiad/onto-chan
  [{:keys [outputs coll]}]
  (clojure.core.async/onto-chan (:out outputs) coll true))

(defmethod csp/construct! :naiad/no-close
  [{:keys [inputs outputs]}]
  (clojure.core.async/pipe (:in inputs) (:out outputs) false))

(defn close-all!
  ([& chans-seq]
   (doseq [chans chans-seq]
     (doseq [c (if (map? chans)
                 (vals chans)
                 chans)]
       (close! c)))))

(defn index-of [coll itm]
  (loop [idx 0
         s (seq coll)]
    (if s
      (if (= (first s) itm)
        idx
        (recur (inc idx) (next s)))
      nil)))

(defmethod csp/construct! :naiad/multiplexer
  [{:keys [inputs outputs]}]
  (let [out (:out outputs)
        selector (:selector inputs)
        closed-c (let [c (async/chan)]
                   (close! c)
                   c)]
    (go (loop []
          (if-some [idx (<! selector)]
            (if-some [v (<! (get inputs idx closed-c))]
              (if (>! out v)
                (recur)
                (close-all! inputs outputs))
              (recur))
            (close-all! inputs outputs))))))

(defmethod csp/construct! :naiad/demultiplexer
  [{:keys [inputs outputs]}]
  (let [selector (:selector outputs)
        in (:in inputs)
        outs (vals (dissoc outputs :selector))]
    (go (loop []
          (if-some [v (<! in)]
            (let [[v c] (async/alts! (map vector outs (repeat v)))
                  idx (index-of outs c)]
              (if v
                (if (>! selector idx)
                  (recur)
                  (close-all! inputs outputs))
                (close-all! inputs outputs)))
            (close-all! inputs outputs))))))

(defmethod csp/construct! :naiad/mapcat-async
  [{:keys [inputs outputs f] :as node}]
  (go
    (let [out (:out outputs)]
      (loop [acc []]
        (if (< (count acc) (count inputs))
          (if-some [v (<! (inputs (count acc)))]
            (recur (conj acc v))
            (close-all! inputs outputs))
          (let [continue? (let [c (async/chan 1)]
                            (apply f c acc)
                            (loop []
                              (if-some [v (<! c)]
                                (if (>! out v)
                                  (recur)
                                  false)
                                true)))]
            (if continue?
              (recur [])
              (close-all! inputs outputs))))))))