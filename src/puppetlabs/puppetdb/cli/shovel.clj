(ns puppetlabs.puppetdb.cli.shovel
  (:import [javax.jms
            ExceptionListener
            JMSException
            MessageListener
            Session
            ConnectionFactory
            Connection
            Queue
            Message]
           [org.apache.activemq ScheduledMessage])
  (:require [clojure.tools.logging :as log]
            [puppetlabs.trapperkeeper.config :as config]
            [puppetlabs.trapperkeeper.logging :as logutils]
            [puppetlabs.puppetdb.config :as conf]
            [puppetlabs.kitchensink.core :as kitchensink]
            [clojure.java.io :as io]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.command.dlo :as dlo]
            [puppetlabs.puppetdb.mq :as mq]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.pprint]
            [puppetlabs.puppetdb.command :as cmd]))

(defn ^Session create-session [^Connection connection]
  (.createSession connection false Session/CLIENT_ACKNOWLEDGE))

(defn ^Queue create-queue [^Session session endpoint]
  (.createQueue session endpoint))

(defn ^Connection create-connection [^ConnectionFactory factory]
  (doto (.createConnection factory)
    (.setExceptionListener
     (reify ExceptionListener
       (onException [this ex]
         (log/error ex "receiver queue connection error"))))
    (.start)))

(defn create-command-consumer
  [discard-dir process-message]
  (let [discard-message (fn [message exception]
                          (dlo/store-failed-message message exception discard-dir))]
    (fn [^Message message]
      ;; When the queue is shutting down, it sends nil message
      (when message
        (try
          (let [msg (mq/convert-jms-message message)]
            (try+
             (let [{:keys [command version annotations] :as parsed-result} (cmd/parse-command msg)
                   id (:id annotations)]
               (try
                 (process-message parsed-result)
                 (catch Exception ex
                   (log/errorf ex "[%s] [%s] [%s] Failed to process message" id command version)
                   (discard-message parsed-result ex))))
             (catch AssertionError ex
               (log/errorf ex "Fatal error parsing command: %s" msg)
               (discard-message msg ex))
             (catch Exception ex
               (log/errorf ex "Fatal error parsing command: %s" msg)
               (discard-message msg ex))))
          (.acknowledge message)
          (catch Exception ex
            (log/error ex "Unable to process message. Message not acknowledged and will be retried")))))))

(defn create-mq-receiver
  [^ConnectionFactory conn-pool endpoint command-consumer]
  (with-open [connection (create-connection conn-pool)
              session (create-session connection)
              consumer (.createConsumer session (create-queue session endpoint))]
    (try
      (loop [msg (.receive consumer 5000)]
        (when msg
          (command-consumer msg)
          (recur (.receive consumer 5000))))
      (catch javax.jms.IllegalStateException e
        (log/info "Received IllegalStateException, shutting down"))
      (catch Exception e
        (log/error e)))))


(defn create-scheduler-receiver
  [^ConnectionFactory conn-pool endpoint command-consumer]
  (with-open [connection (create-connection conn-pool)
              session (create-session connection)]
    (let [request-browse (.createTopic session ScheduledMessage/AMQ_SCHEDULER_MANAGEMENT_DESTINATION)
          browse-dest (.createTemporaryQueue session)]
      (with-open [producer (.createProducer session request-browse)
                  consumer (.createConsumer session browse-dest)]
        (let [request (doto (.createMessage session)
                        (.setStringProperty ScheduledMessage/AMQ_SCHEDULER_ACTION
                                            ScheduledMessage/AMQ_SCHEDULER_ACTION_BROWSE)
                        (.setJMSReplyTo browse-dest))]
          (.send producer request)
          (Thread/sleep 2000)
          (log/info "Processing messages from the scheduler")
          (try
            (loop [msg (.receive consumer 5000)]
              (when msg
                (command-consumer msg)
                (recur (.receive consumer 5000))))
            (catch javax.jms.IllegalStateException e
              (log/info "Received IllegalStateException, shutting down"))
            (catch Exception e
              (log/error e))))))))

(defn process-message [msg] (clojure.pprint/pprint msg))
(def default-mq-endpoint "puppetlabs.puppetdb.commands")
(def retired-mq-endpoint "com.puppetlabs.puppetdb.commands")
(def cli-description "Transfer messages from ActiveMQ to Stockpile")

(defn- validate-cli!
  [args]
  (let [specs [["-c" "--config CONFIG" "Path to config or conf.d directory (required)"
                :parse-fn (comp conf/process-config!
                                conf/adjust-and-validate-tk-config
                                config/load-config)]]
        required [:config]]
    (utils/try+-process-cli!
     #(kitchensink/cli! args specs required))))

(defn- transfer-old-messages! [mq-endpoint]
  (let [[pending exists?]
        (try+
         [(mq/queue-size "localhost" retired-mq-endpoint) true]
         (catch [:type ::mq/queue-not-found] ex [0 false]))]
    (when (pos? pending)
      (log/info (format "Transferring %d commands from legacy queue" pending))
      (let [n (mq/transfer-messages! "localhost" retired-mq-endpoint mq-endpoint)]
        (log/info (format "Transferred %d commands from legacy queue" n))))
    (when exists?
      (mq/remove-queue! "localhost" retired-mq-endpoint)
      (log/info "Removed legacy queue"))))

(defn -main
  [& args]
  (let [[{{global-config :global
           {mq-config :mq :as cmd-proc-config} :command-processing} :config}]
        (validate-cli! args)]
    (logutils/configure-logging! (:logging-config global-config))
    (let [mq-dir (str (io/file (:vardir global-config) "mq"))
          mq-broker-url (format "%s&wireFormat.maxFrameSize=%s&marshal=true"
                                (:address mq-config "vm://localhost?jms.prefetchPolicy.all=1&create=false")
                                (:max-frame-size cmd-proc-config 209715200))
          mq-discard-dir (str (io/file mq-dir "discard"))
          mq-endpoint (:endpoint mq-config default-mq-endpoint)
          conn-pool (mq/activemq-connection-factory mq-broker-url)
          broker (try
                   (log/info "Starting broker")
                   (mq/build-and-start-broker! "localhost" mq-dir cmd-proc-config)
                   (catch java.io.EOFException e
                     (log/error e "EOF Exception caught during broker start, this might be due to KahaDB corruption. Consult the PuppetDB troubleshooting guide.")
                     (throw e)))
          command-consumer (create-command-consumer mq-discard-dir process-message)]
      (create-mq-receiver conn-pool mq-endpoint command-consumer)
      (create-scheduler-receiver conn-pool mq-endpoint command-consumer))))
