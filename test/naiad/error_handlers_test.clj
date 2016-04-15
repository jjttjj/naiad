(ns naiad.error-handlers-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [naiad :as n]
            [naiad.error-handlers :refer [log-to-chan abort]]))

(deftest error-log-test
  (let [log (async/chan 1000)]
    (is (= (n/flow-result
             (n/with-annotations {:error-fn (log-to-chan log)}
               (n/filter (fn [x]
                           (assert (odd? x))
                           true)
                 (range 10))))
          (filter odd? (range 10))))))


(deftest abort-test
  (is (= (n/flow-result
           (n/with-annotations {:error-fn abort}
             (n/filter (fn [x]
                         (assert (odd? x))
                         true)
               [1 3 5 6 7])))
        [1 3 5])))

