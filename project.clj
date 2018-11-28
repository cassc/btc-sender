(defproject btc-sender "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.taoensso/timbre "4.7.4"]
                 [org.clojure/core.async "0.2.395"]
                 [org.bitcoinj/bitcoinj-core "0.14.7"]]
  :main ^:skip-aot btc-sender.core)
