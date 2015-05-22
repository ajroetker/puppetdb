(ns puppetlabs.puppetdb.version-test
  (:import (java.util.concurrent TimeUnit))
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [clojure.test :refer :all]
            [puppetlabs.puppetdb.testutils :refer [get-request deftestseq]]
            [puppetlabs.puppetdb.version :as version]
            [puppetlabs.puppetdb.fixtures :as fixt]
            [puppetlabs.puppetdb.version.core :as version-core]
            [puppetlabs.trapperkeeper.testutils.logging :refer [with-log-output]]))

(use-fixtures :each fixt/with-test-db)

(def endpoints [[:v1 "/v1"]])

(def parsed-body
  "Returns clojure data structures from the JSON body of
   ring response."
  (comp json/parse-string :body))

(defn with-version-app
  [request overrides]
  (let [app (-> (merge {:product-name "puppetdb"
                        :update-server "FOO"}
                       overrides)
                version/build-app)]
    (app request)))

(deftestseq test-latest-version
  [[version endpoint] endpoints]

  (with-redefs [version-core/update-info
                (constantly
                 {"newer" true
                  "link" "http://docs.puppetlabs.com/puppetdb/100.0/release_notes.html"
                  "version" "100.0.0"})
                version-core/version (constantly "99.0.0")]
    (testing "should return 'newer'->true if product is not specified"
      (let [response (-> (get-request (str endpoint "/latest"))
                         with-version-app
                         parsed-body)]

        (are [expected response-key] (= expected
                                        (get response response-key))
             true "newer"
             "100.0.0" "version"
             "http://docs.puppetlabs.com/puppetdb/100.0/release_notes.html" "link")))
    (testing "should return 'newer'->true if product is 'puppetdb"
      (let [response (-> (get-request (str endpoint "/latest"))
                         (with-version-app {:product-name "puppetdb"})
                         parsed-body)]
        (are [expected response-key] (= expected
                                        (get response response-key))
             true "newer"
             "100.0.0" "version"
             "http://docs.puppetlabs.com/puppetdb/100.0/release_notes.html" "link")))
    (testing "should return 'newer'->false if product is 'pe-puppetdb"
      ;; it should *always* return false for pe-puppetdb because
      ;; we don't even want to allow checking for updates
      (let [response (-> (get-request (str endpoint "/latest"))
                         (with-version-app {:product-name "pe-puppetdb"})
                         parsed-body)]
        (are [expected response-key] (= expected
                                        (get response response-key))
             false "newer"
             "99.0.0" "version"
             nil "link")))))

(deftestseq test-latest-version
  [[version endpoint] endpoints]

  (testing "shouldn't log HTTP errors hitting update server at INFO"
    (with-log-output logz
      (let [response (-> (get-request (str endpoint "/latest"))
                         (with-version-app {:update-server "http://known.invalid.domain"
                                            :scf-read-db fixt/*db*}))
            log-levels-emitted (set (map second @logz))]
        (is (nil? (log-levels-emitted :info)))))))
