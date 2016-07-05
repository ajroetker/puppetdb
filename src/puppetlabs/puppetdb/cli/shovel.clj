(ns puppetlabs.puppetdb.cli.shovel
  (:import [javax.jms
            ExceptionListener
            JMSException
            MessageListener
            Session
            TextMessage
            BytesMessage
            ConnectionFactory
            Connection
            MessageConsumer
            Queue
            Message]
           [org.apache.activemq.broker BrokerService]
           [org.apache.activemq.usage SystemUsage MemoryUsage]
           [org.apache.activemq.pool PooledConnectionFactory]
           [org.apache.activemq ActiveMQConnectionFactory ScheduledMessage])
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [puppetlabs.i18n.core :as i18n]
            [puppetlabs.trapperkeeper.config :as config]
            [puppetlabs.trapperkeeper.logging :as logutils]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.config :as conf]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.command.dlo :as dlo]
            [puppetlabs.puppetdb.command :as cmd]
            [clojure.pprint]))

(def cli-description "Transfer messages from ActiveMQ to Stockpile")

(defn ^Session create-session [^Connection connection]
  (.createSession connection false Session/CLIENT_ACKNOWLEDGE))

(defn ^Queue create-queue [^Session session endpoint]
  (.createQueue session endpoint))

(defn ^Connection create-connection [^ConnectionFactory factory]
  (doto (.createConnection factory)
    (.setExceptionListener
     (reify ExceptionListener
       (onException [this ex]
         (log/error ex (i18n/trs "receiver queue connection error")))))
    (.start)))

(defn extract-headers
  "Creates a map of custom headers included in `message`, currently only
  supports String headers."
  [^Message msg]
  (reduce (fn [acc k]
            (assoc acc
              (keyword k)
              (.getStringProperty msg k)))
          {} (enumeration-seq (.getPropertyNames msg))))

(defn convert-message-body
  "Convert the given `message` to a string using the type-specific method."
  [^Message message]
  (cond
   (instance? javax.jms.TextMessage message)
   (let [^TextMessage text-message message]
     (.getText text-message))
   (instance? javax.jms.BytesMessage message)
   (let [^BytesMessage bytes-message message
         len (.getBodyLength message)
         buf (byte-array len)
         n (.readBytes bytes-message buf)]
     (when (not= len n)
       (throw (Exception. (i18n/trs "Only read {0}/{1} bytes from incoming message" n len))))
     (String. buf "UTF-8"))
   :else
   (throw (Exception. (i18n/trs "Expected TextMessage or BytesMessage; found {0}"
                                (class message))))))

(defn convert-jms-message [m]
  {:headers (extract-headers m) :body (convert-message-body m)})

(defn create-message-processor
  [discard-dir stockpiler-fn]
  (let [discard-message (fn [message exception]
                          (dlo/store-failed-message message exception discard-dir))]
    (fn [^Message message]
      ;; When the queue is shutting down, it sends nil message
      (when message
        (try
          (let [msg (convert-jms-message message)]
            (try
              (let [{:keys [command version annotations] :as parsed-result} (cmd/parse-command msg)
                    id (:id annotations)]
                (try
                  (stockpiler-fn parsed-result)
                  (catch Exception ex
                    (log/error ex (i18n/trs "[{0}] [{1}] [{2}] Unable to process message" id command version))
                    (discard-message parsed-result ex))))
              (catch AssertionError ex
                (log/error ex (i18n/trs "Unable to process message: {0}" msg))
                (discard-message message ex))
              (catch Exception ex
                (log/error ex (i18n/trs "Unable to process message: {0}" msg))
                (discard-message message ex))))
          (catch Exception ex
            (log/error ex (i18n/trs "Unable to process message: {0}" message))
            (discard-message message ex))
          (finally
            (.acknowledge message)))))))

(defn consume-everything [^MessageConsumer consumer process-message]
  (try
    (loop []
      (when-let [msg (.receive consumer 5000)]
        (process-message msg)
        (recur)))
    (catch javax.jms.IllegalStateException e
      (log/info (i18n/trs "Received IllegalStateException, shutting down")))
    (catch Exception e
      (log/error e))))

(defn create-mq-receiver
  [^ConnectionFactory conn-pool endpoint process-message]
  (with-open [connection (create-connection conn-pool)
              session (create-session connection)
              consumer (.createConsumer session (create-queue session endpoint))]
    (log/info (i18n/trs "Processing messages from the Queue: {0}" endpoint))
    (consume-everything consumer process-message)))

(defn create-scheduler-receiver
  [^ConnectionFactory conn-pool endpoint process-message]
  (with-open [connection (create-connection conn-pool)
              session (create-session connection)]
    (let [request-browse (.createTopic session ScheduledMessage/AMQ_SCHEDULER_MANAGEMENT_DESTINATION)
          browse-dest (.createTemporaryQueue session)]
      (with-open [producer (.createProducer session request-browse)
                  consumer (.createConsumer session browse-dest)]
        (let [request (doto (.createMessage session)
                        (.setStringProperty ScheduledMessage/AMQ_SCHEDULER_ACTION
                                            ScheduledMessage/AMQ_SCHEDULER_ACTION_BROWSE)
                        (.setJMSReplyTo browse-dest))
              process-message-and-remove (fn [^Message msg]
                                           (try
                                             (process-message msg)
                                             (let [msg-id (.getStringProperty msg
                                                                              ScheduledMessage/AMQ_SCHEDULED_ID)
                                                   remove (doto (.createMessage session)
                                                            (.setStringProperty ScheduledMessage/AMQ_SCHEDULER_ACTION
                                                                                ScheduledMessage/AMQ_SCHEDULER_ACTION_REMOVE)
                                                            (.setStringProperty ScheduledMessage/AMQ_SCHEDULED_ID msg-id))]
                                               (.send producer remove))
                                             (catch Exception e nil)))]
          (.send producer request)
          (Thread/sleep 2000)
          (log/info (i18n/trs "Processing messages from the Scheduler"))
          (consume-everything consumer process-message-and-remove))))))

(def default-mq-endpoint "puppetlabs.puppetdb.commands")
(def retired-mq-endpoint "com.puppetlabs.puppetdb.commands")

(defn stockpiler [msg] (clojure.pprint/pprint msg))

(defn- set-usage!
  "Internal helper function for setting `SystemUsage` values on a `BrokerService`
  instance.

  `broker`    - the `BrokerService` instance
  `megabytes` - the value to set as the limit for the desired `SystemUsage` setting
  `usage-fn`  - a function that accepts a `SystemUsage` instance and returns
  the child object whose limit we are configuring.
  `desc`      - description of the setting we're configuring, to be used in a log message
  "
  [^BrokerService broker
   megabytes
   usage-fn
   ^String desc]
  {:pre [((some-fn nil? integer?) megabytes)
         (fn? usage-fn)]}
  (when megabytes
    (log/info (i18n/trs "Setting ActiveMQ {0} limit to {1} MB" desc megabytes))
    (-> broker
        (.getSystemUsage)
        (usage-fn)
        (.setLimit (* megabytes 1024 1024))))
  broker)

(defn ^:dynamic enable-jmx
  "This function exists to enable starting multiple PuppetDB instances
  inside a single JVM. Starting up a second instance results in a
  collision exception between JMX beans from the two
  instances. Disabling JMX from the broker avoids that issue"
  [broker should-enable?]
  (.setUseJmx broker should-enable?))

(defn ^BrokerService build-embedded-broker
  "Configures an embedded, persistent ActiveMQ broker.

  `name` - What to name to queue. As this is an embedded broker, the
  full name will be of the form 'vm://foo' where 'foo' is the name
  you've supplied. That is the full URI you should use for
  establishing connections to the broker.

  `dir` - What directory in which to store the broker's data files. It
  will be created if it doesn't exist.

  `config` - an optional map containing configuration values for initializing
  the broker.  Currently supported options:

  :store-usage  - the maximum amount of disk usage to allow for persistent messages, or `nil` to use the default value of 100GB.
  :temp-usage   - the maximum amount of disk usage to allow for temporary messages, or `nil` to use the default value of 50GB.
  :memory-usage - the maximum amount of disk usage to allow for persistent messages, or `nil` to use the default value of 1GB.
  "
  [^String name ^String dir {:keys [memory-usage store-usage temp-usage] :as config}]
  {:pre [(map? config)]}
  (let [mq (doto (BrokerService.)
             (.setUseShutdownHook false)
             (.setBrokerName name)
             (.setDataDirectory dir)
             (.setSchedulerSupport true)
             (.setPersistent true)
             (enable-jmx true)
             (set-usage! memory-usage #(.getMemoryUsage %) "MemoryUsage")
             (set-usage! store-usage #(.getStoreUsage %) "StoreUsage")
             (set-usage! temp-usage #(.getTempUsage %) "TempUsage"))
        mc (doto (.getManagementContext mq)
             (.setCreateConnector false))
        db (doto (.getPersistenceAdapter mq)
             (.setIgnoreMissingJournalfiles true)
             (.setArchiveCorruptedIndex true)
             (.setCheckForCorruptJournalFiles true)
             (.setChecksumJournalFiles true))]
    mq))

(defn ^BrokerService start-broker!
  "Starts up the supplied broker, making it ready to accept
  connections."
  [^BrokerService broker]
  (doto broker
    (.start)
    (.waitUntilStarted)))

(defn stop-broker!
  "Stops the supplied broker"
  [^BrokerService broker]
  (doto broker
    (.stop)
    (.waitUntilStopped)))

(defn ^BrokerService retry-build-and-start-broker!
  [brokername dir config]
  (try
    (start-broker! (build-embedded-broker brokername dir config))
    (catch java.io.EOFException e
      (log/error e (i18n/trs "EOF Exception caught during broker start, this might be due to KahaDB corruption. Consult the PuppetDB troubleshooting guide."))
      (throw e))))

(defn ^BrokerService build-and-start-broker!
  "Builds ands starts a broker in one go, attempts restart upon known exceptions"
  [brokername dir config]
  (try
    (try
      (start-broker! (build-embedded-broker brokername dir config))
      (catch java.io.EOFException e
        (log/warn (i18n/trs "Caught EOFException on broker startup, trying again. This is probably due to KahaDB corruption (see \"KahaDB Corruption\" in the PuppetDB manual)."))
        (retry-build-and-start-broker! brokername dir config)))
    (catch java.io.IOException e
      (throw (java.io.IOException.
              (i18n/trs "Unable to start broker in {0}. This is probably due to KahaDB corruption or version incompatibility after a PuppetDB downgrade (see \"KahaDB Corruption\" in the PuppetDB manual)." (pr-str dir))
              e)))))

(defn activemq-connection-factory [spec]
  (proxy [PooledConnectionFactory java.lang.AutoCloseable]
      [(ActiveMQConnectionFactory. spec)]
    (close [] (.stop this))))

(defn activemq->stockpile
  [{global-config :global
    {mq-config :mq :as cmd-proc-config} :command-processing}]
  ;; Starting
  (let [mq-dir (str (io/file (:vardir global-config) "mq"))
        mq-broker-url (format "%s&wireFormat.maxFrameSize=%s&marshal=true"
                              (:address mq-config "vm://localhost?jms.prefetchPolicy.all=1&create=false")
                              (:max-frame-size cmd-proc-config 209715200))
        mq-discard-dir (str (io/file mq-dir "discard"))
        mq-endpoint (:endpoint mq-config default-mq-endpoint)
        conn-pool (activemq-connection-factory mq-broker-url)
        broker (do (log/info (i18n/trs "Starting broker"))
                   (build-and-start-broker! "localhost" mq-dir cmd-proc-config))
        process-message (create-message-processor mq-discard-dir stockpiler)]

    ;; Real work
    (create-scheduler-receiver conn-pool mq-endpoint process-message)
    (create-mq-receiver conn-pool mq-endpoint process-message)
    (create-mq-receiver conn-pool retired-mq-endpoint process-message)

    ;; Stopping
    (log/info (i18n/trs "Stopping ActiveMQConnectionFactory"))
    (.stop conn-pool)
    (log/info (i18n/trs "Stopping broker"))
    (stop-broker! broker)
    (log/info (i18n/trs "You may safely delete {0}" mq-dir))))

(defn- validate-cli!
  [args]
  (let [specs [["-c" "--config CONFIG" "Path to config or conf.d directory (required)"
                :parse-fn (comp conf/process-config!
                                conf/adjust-and-validate-tk-config
                                config/load-config)]]
        required [:config]]
    (utils/try+-process-cli!
     #(kitchensink/cli! args specs required))))

(defn -main
  [& args]
  (let [[{:keys [config]}] (validate-cli! args)]
    (logutils/configure-logging! (:logging-config (:global config)))
    (activemq->stockpile config)))
