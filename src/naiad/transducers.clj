(ns naiad.transducers
  (:require [naiad.transducers.ioc :refer [transducer ingest emit if-value]])
  (:refer-clojure :exclude [first nth last reduce]))



(defn reduce [rf init]
  (transducer
    (loop [acc init]
      (if-value [v (ingest)]
        (let [acc (rf acc v)]
          (if (reduced? acc)
            (emit acc)
            (recur acc)))
        (emit acc)))))

(def ^{:doc "A transducer that filters all but the first item in a tansduction"}
  first
  (take 1))

(defn nth
  "Creates a transducer that filters all but the nth item of a transduction"
  [idx]
  (comp (drop idx)
        first))

(def ^{:doc "A transducer that filters all but the last item in a transduction"}
  last
  (transducer
    (loop [have-item? false
           item nil]
      (if-value [v (ingest)]
        (recur true v)
        (when have-item?
          (emit item))))))
