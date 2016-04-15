(ns naiad.error-handlers
  (:require [clojure.core.async :as async]
            [clojure.stacktrace :as stacktrace]))

(defn ignore [^Throwable t]
  (async/go :error/continue))

(defn log-to-chan [c]
  (fn [^Throwable t]
    (async/go
      (async/>! c t)
      :error/continue)))

(defn print-and-continue [^Throwable t]
  (async/thread
    (stacktrace/print-stack-trace t)
    :error/continue))

(defn abort [^Throwable t]
  (async/go :error/abort))