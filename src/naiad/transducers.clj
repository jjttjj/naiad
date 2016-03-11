(ns naiad.transducers
  (:refer-clojure :exclude [first second nfirst]))




(defn make-transducer [f]
  (fn [xf]
    (fn
      ([] (xf))
      ([acc] (xf acc))
      ([acc itm]
        (f acc itm)))))

(def first (make-transducer (fn [acc item]
                              (reduced item))))