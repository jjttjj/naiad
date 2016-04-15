(ns naiad.transducers-test
  (:require [naiad.transducers :as tr]
            [clojure.test :refer :all]))


(deftest reduce-test
  (is (= (into #{}
               (tr/reduce conj [])
               (range 3))
         #{(vec (range 3))}))

  (is (= (into #{}
               (comp (take 5)
                     (tr/reduce conj []))
               (range 10))
         #{(vec (range 5))}))

  (is (= (into #{}
               (comp (take 5)
                     (tr/reduce conj [])
                     (take 1))
               (range 10))
         #{(vec (range 5))})))

(defn transient-conj
  ([] (transient []))
  ([acc] (persistent! acc))
  ([acc itm] (conj acc itm)))

(deftest first-tests
  (is (= (into #{} tr/first (range 10))
         #{0}))

  (is (= (into #{} tr/first [])
         #{})))

(deftest nth-tests
  (is (= (into #{} (tr/nth 2) (range 10))
        #{2}))
  (is (= (into #{} (tr/nth 0) (range 10))
        #{0}))
  (is (= (into #{} (tr/nth 100) (range 10))
        #{})))

(deftest last-tests
  (is (= (into #{} tr/last (range 10))
        #{9}))

  (is (= (into #{} tr/last [])
        #{}))

  (is (= (into #{} (comp (take 3) tr/last) (range))
        #{2})))

#_(deftest transduce-test
  (is (= (into #{}
               (tr/transduce (map inc)
                             conj [])
               (range 3))
         #{(mapv inc (range 3))}))


  (is (= (into #{}
               (tr/transduce (map inc)
                             conj)
               (range 3))
         #{(mapv inc (range 3))}))

  (is (= (into #{}
               (tr/transduce (take 3)
                             conj)
               (range 10))
         #{(vec (range 3))}))
  )

