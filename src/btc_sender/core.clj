(ns btc-sender.core
  (:require
   [taoensso.timbre :as t]
   [clojure.java.io      :as io]
   [clojure.string       :as s]
   [clojure.core.async   :as a :refer [go-loop <! <!! timeout go chan sliding-buffer >!!]])
  (:import
   [java.nio.charset Charset]
   [java.io File]
   [org.bitcoinj.script ScriptOpCodes ScriptBuilder]
   [org.bitcoinj.wallet Wallet]
   [org.bitcoinj.core NetworkParameters ECKey DumpedPrivateKey Coin Transaction Address
    Transaction$SigHash Coin TransactionOutput TransactionOutPoint PeerGroup TransactionBroadcast
    TransactionBroadcast$ProgressCallback Sha256Hash]
   [org.bitcoinj.wallet.listeners WalletCoinsReceivedEventListener WalletCoinsSentEventListener]
   [org.bitcoinj.kits WalletAppKit]
   [org.bitcoinj.params MainNetParams TestNet3Params]))

;; 一些配置
(defonce btc-sender-config
  {:private "你的私钥"
   :store "SPV钱包本地缓存地址"
   :file-prefix "spv"
   :create-time-millis 1533772497726})

;; 广播超时设置
(defonce send-timeout 60000)

;; 用于缓存钱包对象，避免重重加载
(defonce wallet-kit-store (atom nil))

;; 消息发送队列，同时只允许一个交易在发送
(defonce msg-sending-chan (chan (sliding-buffer 100)))

;; 将字符串格式的私钥转换为对象
(defn- string->prvkey [^NetworkParameters network ss]
  (.getKey (DumpedPrivateKey/fromBase58 network ss)))

;; 使用上面的配置启动钱包服务，本方法会在启动成功前阻塞当前线程
(defn start-wallet-service! []
  (let [file-store (io/file (btc-sender-config :store))
        file-prefix (btc-sender-config :file-prefix)
        network (TestNet3Params/get)
        create-time-secs (int (/ (btc-sender-config :create-time-millis) 1000))
        wif (btc-sender-config :private)
        prv-key (string->prvkey network wif)
        pub-key (.getPubKeyHash prv-key)
        address (Address. network pub-key)
        kit (WalletAppKit. ^NetworkParameters network ^File file-store ^String file-prefix)]
    (try
      (.. kit startAsync awaitRunning)
      (let [wallet (.wallet kit)]
        (doto wallet
          ;; create-time-secs的时间设应该设为比本地址的第一次交易时间稍早点
          (.addWatchedAddress address create-time-secs)
          ;; 收到币的事件
          (.addCoinsReceivedEventListener
           (reify WalletCoinsReceivedEventListener
             (onCoinsReceived [this wallet tx prev-balance new-balance]
               (t/info "coin received: from" prev-balance "to" new-balance))))
          ;; 发送币的事件
          (.addCoinsSentEventListener
           (reify WalletCoinsSentEventListener
             (onCoinsSent [this wallet tx prev-balance new-balance]
               (t/info "coin sent:" prev-balance "to" new-balance)))))
        ;; 加入缓存
        (swap! wallet-kit-store assoc :testnet {:kit kit
                                                :o-network network
                                                :wallet wallet
                                                :stop-fn (fn [] (.stopAsync kit))
                                                :balance-fn (fn [] (.-value (.getBalance wallet)))
                                                :unspent-fn (fn [] (.getUnspents wallet))
                                                :address address
                                                :prv-key prv-key
                                                :pub-key pub-key}))
      (t/info "Btc Wallet Service start success! Current balance:" (.getBalance (.wallet kit)) " SAT")
      (catch Exception e
        (t/error e)
        (t/error "walletappkit start error, stopping service ...")
        (.stopAsync kit)))))

;; 根据交易包的大小计算手续费，后面会用到
(defn- calculate-fee [msg-size input-count fee-factor]
  (* fee-factor (+ msg-size (* input-count 148) 34 20)))

;; 发送消息
(defn send-msg! [{:keys [^String msg progress-callback network]
                  :as params}]
  ;; 通过pre前置条件要求调用时钱包已初始化
  {:pre [(#{:mainnet :testnet} network) (network @wallet-kit-store) msg]}
  (let [ch (chan)]
    (try
      (let [fee-factor 1
            {:keys [o-network kit wallet balance-fn unspent-fn address prv-key]} (network @wallet-kit-store)
            tx (Transaction. o-network)
            msg-bytes (.getBytes msg (Charset/forName "UTF-8"))
            msg-script (.. (ScriptBuilder.)
                           (op ScriptOpCodes/OP_RETURN)
                           (data msg-bytes)
                           (build))
            x-unspent (unspent-fn) ;; UTXO
            in-value (reduce (fn [sum unspent] (+ sum (.-value (.getValue unspent)))) 0 x-unspent)
            fee (calculate-fee (count msg-bytes) (count x-unspent) fee-factor)
            peer-group ^PeerGroup (.peerGroup kit)]
        ;; 创建一个发送给自己的交易，这里添加交易的输出
        (doto tx
          (.addOutput (Coin/valueOf 0) msg-script)
          (.addOutput (Coin/valueOf (- in-value fee)) address))
        ;; 从UTXO中组建交易的输入并签名
        (doseq [unspent x-unspent]
          (let [out-point (TransactionOutPoint. ^NetworkParameters o-network ^long (.getIndex unspent) ^Sha256Hash (.getParentTransactionHash unspent))]
            (.addSignedInput tx ^TransactionOutPoint out-point (.getScriptPubKey unspent) prv-key org.bitcoinj.core.Transaction$SigHash/ALL true)))
        ;; 广播签名后的数据
        (let [txid (.getHashAsString tx)]
          (.. peer-group
              (broadcastTransaction tx)
              (setProgressCallback (reify TransactionBroadcast$ProgressCallback
                                     (onBroadcastProgress [req progress]
                                       (when progress-callback
                                         (progress-callback req progress))
                                       (when (> progress 0.9999999)
                                         (t/info "Send msg done:" txid)
                                         (a/put! ch {:success :success :txid txid}))))))
          (t/info "Sending btc msg: " txid msg)))
      (catch Throwable e
        (t/error "Sending btc req failed" params)
        (t/error e)
        (a/put! ch {:error e})))
    (let [[val c] (a/alts!! [ch (timeout send-timeout)])]
      (or val {:error :timeout}))))

;; 判断服务是否已经启动
(defn- service-ready? [network]
  (get @wallet-kit-store network))

;; 如果服务已启动发送消息
(defn send-msg-if-ready [req]
  (if (service-ready? :testnet)
    (send-msg! (assoc req :network :testnet))
    {:error :service-not-ready}))

(comment
  (start-wallet-service!)
  (send-msg-if-ready {:msg "少壮不努力，老大徒伤悲"})
  )
