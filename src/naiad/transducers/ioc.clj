(ns naiad.transducers.ioc
  (:refer-clojure :exclude [first second nfirst reduce into transduce])
  (:require [clojure.core.async.impl.ioc-macros :as ioc]))




(def XF-IDX ioc/USER-START-IDX)
(def ACC-IDX (inc ioc/USER-START-IDX))
(def STARTED-IDX (+ 2 ioc/USER-START-IDX))

(defn ingest []
  (throw (ex-info "Can't call ingest outside of a transducer macro" {})))

(defn emit [val]
  (throw (ex-info "Can't call emit outside of a transducer macro" {})))

(defn -ingest [state blk]
  (ioc/aset-object state ioc/STATE-IDX blk)
  ::ingest)

(defn -emit [state blk v]
  (ioc/aset-all! state ioc/STATE-IDX blk)
  (let [xf (ioc/aget-object state XF-IDX)
        acc (ioc/aget-object state ACC-IDX)
        result (xf acc v)]
    (ioc/aset-all! state ACC-IDX result)
    (when-not (reduced? result)
      :recur)))

(defn -return [state val]
  (ioc/aset-all! state ACC-IDX (reduced (ioc/aget-object state ACC-IDX))))

(def transducer-custom-terminators
  {`ingest `-ingest
   `emit `-emit
   :Return `-return})

(defmacro if-value
  ([[v test] then]
   `(let [~v ~test]
      (if (identical? ~v ::end)
        nil
        ~then)))
  ([[v test] then else]
   `(let [~v ~test]
      (if (identical? ~v ::end)
        ~else
        ~then))))


(defmacro transducer
  [& body]
  `(fn [xf#]
     (let [f# ~(ioc/state-machine `(do ~@body) 3 (keys &env) transducer-custom-terminators)
           state# (-> (f#)
                    (ioc/aset-all! ioc/BINDINGS-IDX (clojure.lang.Var/getThreadBindingFrame)
                      STARTED-IDX false))]
       (fn
         ([] (xf#))
         ([acc#]
          (ioc/aset-all! state# ACC-IDX acc# XF-IDX xf# ioc/VALUE-IDX ::end)
          (ioc/run-state-machine state#)
          (xf# (unreduced (ioc/aget-object state# ACC-IDX))))
         ([acc# itm#]
          (ioc/aset-all! state# XF-IDX xf# ACC-IDX acc#)
          (when-not (ioc/aget-object state# STARTED-IDX)
            (ioc/run-state-machine state#)
            (ioc/aset-all! state# STARTED-IDX true))
          (ioc/run-state-machine
            (ioc/aset-all! state# ioc/VALUE-IDX itm#))
          (ioc/aget-object state# ACC-IDX))))))


