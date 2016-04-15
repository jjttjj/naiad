(ns naiad.metrics)


(defprotocol IMetricsLogger
  (counter [t metric-name delta])
  (guage [t metric-name value-fn])
  (histogram [t metric-name value])
  (meter [t metric-name n-events]))


(extend-protocol IMetricsLogger
  nil
  (counter [t metric-name delta]
    nil)
  (guage [t metric-name value-fn]
    nil)
  (histogram [t metric-name value]
    nil)
  (meter [t metric-name n-events]
    nil)

  clojure.lang.IFn
  (counter [f metric-name delta]
    (f :counter metric-name delta))
  (guage [f metric-name value-fn]
    (f :guage metric-name value-fn))
  (histogram [f metric-name value]
    (f :histogram metric-name value))
  (meter [f metric-name n-events]
    (f :meter metric-name n-events)))