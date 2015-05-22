(ns puppetlabs.puppetdb.version
  (:require [clojure.tools.logging :as log]
            [net.cgrand.moustache :as moustache]
            [puppetlabs.puppetdb.middleware
             :refer [verify-accepts-json wrap-with-globals
                     validate-no-query-params wrap-with-puppetdb-middleware]]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [compojure.core :as compojure]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.version.core :as v]))

(defn current-version
  "Responds with the current version of PuppetDB as a JSON object containing a
  `version` key."
  [version]
  (fn [_]
    (if version
      (http/json-response {:version version})
      (http/error-response "Could not find version" 404))))

(defn latest-version
  "Responds with the latest version of PuppetDB as a JSON object containing a
  `version` key with the version, as well as a `newer` key which is a boolean
  specifying whether the latest version is newer than the current version."
  [{:keys [update-server product-name scf-read-db]}]
  {:pre [(and update-server product-name)]}
  (fn [_]
    (try
      ;; if we get one of these requests from pe-puppetdb, we always want to
      ;; return 'newer->false' so that the dashboard will never try to
      ;; display info about a newer version being available
      (if (= product-name "pe-puppetdb")
        (http/json-response {:newer false :version (v/version) :link nil})
        (if-let [result (v/update-info update-server scf-read-db)]
          (http/json-response result)
          (do (log/debugf "Unable to determine latest version via update-server: '%s'" update-server)
              (http/error-response "Could not find version" 404))))

      (catch java.io.IOException e
        (log/debugf "Error when checking for latest version: %s" e)
        (http/error-response
         (format "Error when checking for latest version: %s" e))))))

(defn routes
  [globals]
  (moustache/app ["v1" ""] {:get (current-version (v/version))}
                 ["v1" "latest"] {:get (latest-version globals)}))
(defn build-app
  [{:keys [authorizer] :as globals}]
  (-> (routes globals)
      verify-accepts-json
      validate-no-query-params
      (wrap-with-puppetdb-middleware authorizer))) 

(defservice version-service
  [[:PuppetDBServer shared-globals]
   [:WebroutingService add-ring-handler get-route]]

  (start [this context]
         (log/info "Starting version service")
         (->> (build-app (shared-globals))
              (compojure/context (get-route this) [])
              (add-ring-handler this))
         context))
