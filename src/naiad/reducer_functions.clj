(ns naiad.reducer-functions
  (:refer-clojure :exclude [first second nfirst]))


(defn first
  ([] nil)
  ([acc] acc)
  ([_ i]
    (reduced i)))


(defn nfirst [n]
  (let [idx (volatile! 0)]
    (fn
      ([] nil)
      ([acc] acc)
      ([acc i]
        (if (= n @idx)
          (reduced i)
          (do (vswap! idx inc)
              acc))))))

(def second (nfirst 1))
(def third (nfirst 2))
(def forth (nfirst 3))