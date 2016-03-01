(ns naiad.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [naiad.core :as df]))

(defmacro flow-result [& expr]
  `(let [p# (promise)]
     (df/flow
       (df/promise-accumulator p# (do ~@expr)))
     @p#))

(deftest map-tests
  (testing "basic map usage"
    (is (= (flow-result
             (df/map inc [1 2 3 4]))
           [2 3 4 5])))

  (testing "map with multiple input"
    (is (= (flow-result
             (df/map vector [1 2 3 4] [5 6 7 8]))
           [[1 5] [2 6] [3 7] [4 8]]))

    (is (= (flow-result
             (df/map + [1 2] [1 2] [1 2] [1 2]))
           [4 8])))

  (testing "map chaining"
    (is (= (flow-result
             (df/map inc (df/map dec (df/map #(+ % %) [1 2 3]))))
           [2 4 6]))

    (is (= (flow-result
             (let [data [1 2 3]]
               (df/map +
                       (df/map dec data)
                       (df/map inc data))))
           [2 4 6]))))

(deftest basic-tansducer-tests
  (testing "filter"
    (is (= (flow-result
             (df/filter even? [1 2 3 4]))
           [2 4])))

  (testing "take"
    (is (= (flow-result
             (df/take 3 (df/filter even? [1 2 3 4 5 6 7 8])))
           [2 4 6])))

  (testing "partition"
    (is (= (flow-result
             (df/partition-all 2 [1 2 3 4]))
           [[1 2] [3 4]]))

    ))


(deftest multi-use-channels
  (testing "data distribution"
    (is (= (flow-result
             (let [filtered (df/filter pos? [1 2])]
               (df/map +
                       (df/map inc filtered)
                       (df/map dec filtered))))
           [2 4]))))


#_(deftest channel-data-sources
  (testing "can use a bare channel as a data source"
    (is (= (let [input (async/to-chan [1 2 3])]
             (flow-result
               (map inc input)))
           [2 3 4]))))