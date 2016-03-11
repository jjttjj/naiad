(ns naiad.reducer-functions-test
  (:require [clojure.test :refer :all]
            [naiad.reducer-functions :as rf]))

(deftest first-test
  (is (= (transduce (map inc) rf/first [1 2 3])
        2))
  (is (= (reduce rf/first nil [1 2 3]))))


(deftest nfirst-test
  (dotimes [x 10]
    (dotimes [y 11]
      (is (= (transduce (map identity) (rf/nfirst y) (range x))
            (first (drop y (range x))))))))



(deftest second-third-forth-tests
  (is (= (transduce (map identity) rf/second (range 10))
        1))
  (is (= (transduce (map identity) rf/third (range 10))
        2))
  (is (= (transduce (map identity) rf/forth (range 10))
        3)))