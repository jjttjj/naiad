(ns naiad.metrics-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [naiad :as df :refer [flow-result]]))


(deftest throughput-metrics
  (let [a (atom [])
        ml (fn [& args]
             (swap! a conj args))]
    (is (= (flow-result
             (df/annotate (df/map inc (range 2))
               :metrics ml))
          [1 2]))

    (is (= @a [[:histogram :buffer-size 1]
               [:histogram :buffer-size 0]
               [:histogram :buffer-size 1]
               [:histogram :buffer-size 0]])))



  (let [a (atom [])
        ml (fn [& args]
             (swap! a conj args))]
    (is (= (flow-result
             (df/with-annotations {:metrics ml}
               (df/map inc (range 2))))
          [1 2]))

    (is (= @a [[:histogram :buffer-size 1]
               [:histogram :buffer-size 0]
               [:histogram :buffer-size 1]
               [:histogram :buffer-size 0]]))))
