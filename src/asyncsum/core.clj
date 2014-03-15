(ns asyncsum.core
  (:gen-class :main true)
  (:use [clojure.java.io :only [reader]]
        [clojure.string :only [split]]
        [clojure.core.async :only [<!! >! chan go close!]]))

(defn read-file
  [filename]
  (let [file-channel (chan)]
    (go
     (with-open [rdr (reader filename)]
        (doseq [line (line-seq rdr)]
          (let [[time value] (split line #"\s+")]
            (>! file-channel [time (BigDecimal. ^String value)]))))
     (close! file-channel))
    file-channel)) 


(defn grouping-stream
  [file-channel]
  (let [grouping-channel (chan)
        fresh-current (fn [value] {:sum value :min value :max value :value value :count 1})]
    (go
     (loop [[time value] (<! file-channel)
            currenttime time
            current (fresh-current value)]
       (if time
         (if (= currenttime time)
           (recur (<! file-channel) 
                  time
                  (assoc current 
                    :sum (+ value (:sum current))
                    :count (+ 1 (:count current))
                    :min (min (:min current) value)
                    :max (max (:max current) value)))
           (do
             (>! grouping-channel [currenttime current])
             (recur (<! file-channel) time (fresh-current value))))
         (>! grouping-channel [currenttime current])))
     (close! grouping-channel))
    grouping-channel))


(defn analyse-stream
  [grouping-channel]
  (println "Time       Value  N_O Roll_Sum Min_Value Max_Value")
  (println "---------------------------------------------------")
  (loop [[time values] (<!! grouping-channel)]
    (when time
      (println (format "%s %s %s  %s  %s   %s" 
                       time 
                       (:value values)
                       (:count values)
                       (:sum values)
                       (:min values)
                       (:max values)))
      (recur (<!! grouping-channel)))))

                            
(defn -main
  [& args]
  (let [filename (first args)
        start (java.util.Date.)]
    (-> filename
        read-file
        grouping-stream
        analyse-stream)
    (println (str (- (.getTime (java.util.Date.)) (.getTime start))))))

    
