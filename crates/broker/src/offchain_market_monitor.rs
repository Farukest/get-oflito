use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use alloy::network::Ethereum;
use alloy::primitives::{Address, U256, Bytes, TxHash, FixedBytes};
use alloy::providers::Provider;
use alloy::signers::{local::PrivateKeySigner, Signer};
use boundless_market::order_stream_client::{order_stream, OrderStreamClient};
use futures_util::StreamExt;
use anyhow::{Context, Result};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket,
    },
};
use alloy::consensus::Transaction;
use crate::config::ConfigLock;
use crate::provers::ProverObj;
use chrono::{DateTime, Utc};
use serde_json::json;
use crate::{errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr}};
use alloy::{
    network::{eip2718::Encodable2718, EthereumWallet, TransactionBuilder},
    providers::ProviderBuilder,
    rpc::types::TransactionRequest,
    primitives::TxKind,
    consensus::{TxEip1559, TxEnvelope},
    sol,
    sol_types::SolCall,
};
use alloy::consensus::SignableTransaction;
use alloy_primitives::Signature;
use boundless_market::ProofRequest;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::SinkExt;
#[derive(Error)]
pub enum OffchainMarketMonitorErr {
    #[error("WebSocket error: {0:?}")]
    WebSocketErr(anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl_coded_debug!(OffchainMarketMonitorErr);

impl CodedError for OffchainMarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            OffchainMarketMonitorErr::WebSocketErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::ReceiverDropped => "[B-OMM-002]",
            OffchainMarketMonitorErr::UnexpectedErr(_) => "[B-OMM-500]",
        }
    }
}

// Global cache'ler - Node.js'teki gibi
static CACHED_CHAIN_ID: AtomicU64 = AtomicU64::new(0);
static CURRENT_NONCE: AtomicU64 = AtomicU64::new(0);
static IS_WAITING_FOR_COMMITTED_ORDERS: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct MonitorConfig {
    pub rust_api_url: String,
    pub allowed_requestors: Option<HashSet<Address>>,
    pub lockin_priority_gas: u64,
    pub min_allowed_lock_timeout_secs: u64,
    pub http_rpc_url: String, // HTTP URL kalsın
}

pub struct OffchainMarketMonitor<P> {
    client: OrderStreamClient,
    signer: PrivateKeySigner,
    prover_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
}

impl<P> OffchainMarketMonitor<P> where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        client: OrderStreamClient,
        signer: PrivateKeySigner,
        prover_addr: Address,
        provider: Arc<P>,
        config_lock: ConfigLock,
    ) -> Self {
        Self {
            client,
            signer,
            prover_addr,
            provider,
            config: config_lock,
        }
    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }


    // Rust API'ye lock verilerini gönder
    async fn send_to_rust_api(rust_api_url: &str, tx_hash: String, lock_block: u64) -> Result<bool, anyhow::Error> {
        let start_time = Instant::now();

        tracing::info!("🦀 Rust API'ye veri gönderiliyor...");
        tracing::info!("📋 TX Hash: {}", tx_hash);
        tracing::info!("📋 Lock Block: {}", lock_block);

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/api/lock-transaction", rust_api_url))
            .header("Content-Type", "application/json")
            .header("Connection", "keep-alive")
            .json(&json!({
                "tx_hash": tx_hash,
                "lock_block": lock_block
            }))
            .send()
            .await
            .context("Failed to send request to Rust API")?;

        let elapsed = start_time.elapsed();
        tracing::info!("⚡ Rust API Response Time: {:.2}ms", elapsed.as_secs_f64() * 1000.0);

        let is_success = response.status().is_success();
        let result: serde_json::Value = response.json().await
            .context("Failed to parse Rust API response")?;

        if is_success && result.get("success").and_then(|v| v.as_bool()).unwrap_or(false) {
            tracing::info!("✅ Rust API success response: {}", result.get("message").and_then(|v| v.as_str()).unwrap_or("OK"));
            Ok(true)
        } else {
            tracing::error!("❌ Rust API error: {}", result.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error"));
            Ok(false)
        }
    }

    // Committed orders sayısını kontrol et
    async fn check_committed_orders(rust_api_url: &str) -> Result<i32, anyhow::Error> {
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/api/committed-orders-count", rust_api_url))
            .header("Content-Type", "application/json")
            .header("Connection", "keep-alive")
            .send()
            .await
            .context("Failed to check committed orders")?;

        let is_success = response.status().is_success();
        let result: serde_json::Value = response.json().await
            .context("Failed to parse committed orders response")?;

        if is_success {
            let count = result.get("count").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
            tracing::info!("📊 Committed orders count: {}", count);
            Ok(count)
        } else {
            tracing::error!("❌ Failed to get committed orders: {}", result.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error"));
            Ok(-1)
        }
    }

    // Committed orders polling başlat
    async fn start_committed_orders_polling(rust_api_url: String) -> Result<(), anyhow::Error> {
        // İlk başta durumu kontrol et
        tracing::info!("🔄 Initial committed orders check...");

        let initial_count = Self::check_committed_orders(&rust_api_url).await.unwrap_or(-1);

        if initial_count == 0 {
            tracing::info!("✅ Initial committed orders = 0! Starting order monitoring immediately...");
            IS_WAITING_FOR_COMMITTED_ORDERS.store(false, Ordering::Relaxed);
            tracing::info!("🧹 Ready for fresh monitoring");
        } else if initial_count > 0 {
            tracing::info!("⏳ Initial committed orders = {}. Waiting for completion...", initial_count);
            IS_WAITING_FOR_COMMITTED_ORDERS.store(true, Ordering::Relaxed);
        } else {
            tracing::error!("⚠️ Initial committed orders check failed. Exiting...");
            return Err(anyhow::anyhow!("Failed to check initial committed orders"));
        }

        // 3 dakikalık interval için async task spawn et
        let rust_api_url_clone = rust_api_url.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3 * 60)); // 3 dakika

            loop {
                interval.tick().await;

                if !IS_WAITING_FOR_COMMITTED_ORDERS.load(Ordering::Relaxed) {
                    continue;
                }

                tracing::info!("🔄 Checking committed orders status...");

                // Committed orders check
                match Self::check_committed_orders(&rust_api_url_clone).await {
                    Ok(committed_count) => {
                        if committed_count == 0 {
                            tracing::info!("🎯 Committed orders = 0! Resuming order monitoring...");
                            IS_WAITING_FOR_COMMITTED_ORDERS.store(false, Ordering::Relaxed);
                            tracing::info!("🧹 Ready for fresh monitoring");
                        } else if committed_count > 0 {
                            tracing::info!("⏳ Still {} committed orders. Waiting...", committed_count);
                        } else {
                            tracing::warn!("⚠️ Error getting committed orders count. Retrying in 3min...");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("⚠️ Failed to check committed orders: {}. Retrying in 3min...", e);
                    }
                }
            }
        });

        Ok(())
    }

    // monitor_orders fonksiyonunda değişiklik
    pub async fn monitor_orders(
        client: OrderStreamClient,
        signer: PrivateKeySigner,
        cancel_token: CancellationToken,
        prover_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
    ) -> Result<(), OffchainMarketMonitorErr> {
        // Config'i başta oku
        let monitor_config = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            MonitorConfig {
                rust_api_url: locked_conf.market.rust_api_url.clone(),
                allowed_requestors: locked_conf.market.allow_requestor_addresses.clone(),
                lockin_priority_gas: locked_conf.market.lockin_priority_gas.unwrap_or(5000000),
                min_allowed_lock_timeout_secs: locked_conf.market.min_lock_out_time * 60,
                http_rpc_url: locked_conf.market.my_rpc_url.clone(), // HTTP URL kullan
            }
        };

        let socket = client.connect_async(&signer).await.map_err(OffchainMarketMonitorErr::WebSocketErr)?;
        let mut stream = order_stream(socket);

        // Chain ID ve initial nonce'u cache'le
        let chain_id = 8453u64;
        CACHED_CHAIN_ID.store(chain_id, Ordering::Relaxed);

        let initial_nonce = provider
            .get_transaction_count(signer.address())
            .pending()
            .await
            .context("Failed to get transaction count")?;

        CURRENT_NONCE.store(initial_nonce, Ordering::Relaxed);

        tracing::info!("Cache initialized - ChainId: {}, Initial Nonce: {}", chain_id, initial_nonce);

        // Committed orders polling'i başlat
        Self::start_committed_orders_polling(monitor_config.rust_api_url.clone()).await
            .map_err(|e| OffchainMarketMonitorErr::UnexpectedErr(e))?;

        tracing::info!("🔄 Committed orders polling initialized");

        let mut is_processing = false;

        loop {
            tokio::select! {
            order_data = stream.next() => {
                match order_data {
                    Some(order_data) => {
                        // Eğer committed orders bekleme modundaysak order monitoring yapma
                        if IS_WAITING_FOR_COMMITTED_ORDERS.load(Ordering::Relaxed) {
                            continue;
                        }

                        if is_processing {
                            tracing::warn!("Zaten bir transaction işleniyor, yeni order beklemede...");
                            continue;
                        }

                        let request_id = order_data.order.request.id;
                        let client_addr = order_data.order.request.client_address();


                        // İzin verilen adres kontrolü
                        if let Some(ref allow_addresses) = monitor_config.allowed_requestors {
                            if !allow_addresses.contains(&client_addr) {
                                tracing::debug!("Client not in allowed requestors, skipping request: 0x{:x}", request_id);
                                continue;
                            }
                        }
                        // **ORDER ID ALINDI - TIMING BAŞLAT**
                        let order_received_time = Instant::now();
                        tracing::info!("ORDER RECEIVED - Request ID: 0x{:x} at {}", request_id, Self::format_time(chrono::Utc::now()));

                        // Lock timeout kontrolü
                        if (order_data.order.request.offer.lockTimeout as u64) < monitor_config.min_allowed_lock_timeout_secs {
                            tracing::info!(
                                "Skipping order {}: Lock Timeout ({} seconds) is less than minimum required ({} seconds).",
                                order_data.order.request.id,
                                order_data.order.request.offer.lockTimeout,
                                monitor_config.min_allowed_lock_timeout_secs
                            );
                            continue;
                        }

                        is_processing = true;


                        // **RAW TRANSACTION GÖNDERİM ÖNCESİ SÜRE HESAPLA**
                        let pre_send_elapsed = order_received_time.elapsed();
                        tracing::info!("PRE-SEND PROCESSING TIME: {:.2}ms for request 0x{:x}",
                        pre_send_elapsed.as_secs_f64() * 1000.0, request_id);
                        // WebSocket ile raw transaction gönder
                        match Self::send_raw_transaction(
                            &order_data,
                            &signer,
                            client.boundless_market_address,
                            &monitor_config,
                            prover_addr,
                            provider.clone(),
                        ).await {
                            Ok(lock_block) => {
                                tracing::info!("LOCK SUCCESS! Request: 0x{:x}, Block: {}", request_id, lock_block);

                                // Block timestamp al
                                let lock_timestamp = match provider
                                    .get_block_by_number(lock_block.into())
                                    .await
                                {
                                    Ok(Some(block)) => block.header.timestamp,
                                    Ok(None) => {
                                        tracing::error!("CRITICAL: Block {} not found after successful lock!", lock_block);
                                        is_processing = false;
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::error!("CRITICAL: Failed to get block {} after successful lock: {:?}", lock_block, e);
                                        is_processing = false;
                                        continue;
                                    }
                                };

                                // Lock price hesapla
                                let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                                    Ok(price) => price,
                                    Err(e) => {
                                        tracing::error!("CRITICAL: Failed to calculate lock price after successful lock: {:?}", e);
                                        is_processing = false;
                                        continue;
                                    }
                                };

                                tracing::info!("Lock successful for request 0x{:x}, price: {}, block: {}",
                                             request_id, lock_price, lock_block);
                                is_processing = false;
                            }
                            Err(err) => {
                                tracing::error!("Transaction error for request: 0x{:x}, error: {}", request_id, err);

                                // Bizim tx kontrolü ve block numarası al
                                match Self::check_our_transaction_and_lock_status(&provider, &signer, client.boundless_market_address, request_id).await {
                                    Ok((true, true, Some(lock_block))) => {
                                        // Bizim transaction başarılı VE request locked + block numarası var
                                        tracing::warn!("Transaction error occurred, but OUR transaction was successful for 0x{:x} at block {}!", request_id, lock_block);

                                        let lock_timestamp = match provider
                                            .get_block_by_number(lock_block.into())
                                            .await
                                        {
                                            Ok(Some(block)) => block.header.timestamp,
                                            Ok(None) => {
                                                tracing::error!("CRITICAL: Lock block {} not found!", lock_block);
                                                is_processing = false;
                                                continue;
                                            }
                                            Err(e) => {
                                                tracing::error!("CRITICAL: Failed to get lock block {}: {:?}", lock_block, e);
                                                is_processing = false;
                                                continue;
                                            }
                                        };

                                        let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                                            Ok(price) => price,
                                            Err(e) => {
                                                tracing::error!("CRITICAL: Failed to calculate lock price for block {}: {:?}", lock_block, e);
                                                is_processing = false;
                                                continue;
                                            }
                                        };

                                        tracing::info!("Our transaction was successful, request 0x{:x} locked at block {} with price {}",
                                                     request_id, lock_block, lock_price);
                                        is_processing = false;
                                    }
                                    Ok((false, true, _)) => {
                                        // Başkası lock'ladı - skipped
                                        tracing::info!("Request 0x{:x} locked by someone else - skipping", request_id);
                                        is_processing = false;
                                    }
                                    Ok((_, false, _)) => {
                                        // Lock yok - gerçekten başarısız
                                        tracing::info!("Request 0x{:x} confirmed NOT locked", request_id);
                                        is_processing = false;
                                    }
                                    Ok((true, true, None)) => {
                                        // Bizim transaction başarılı VE request locked + block numarası gelmedi
                                        tracing::info!("true, true, None : Request 0x{:x} confirmed NOT locked", request_id);
                                        is_processing = false;
                                    }
                                    Err(check_err) => {
                                        tracing::error!("Failed to check transaction and lock status for 0x{:x}: {:?}", request_id, check_err);
                                        is_processing = false;
                                    }
                                }
                            }
                        }

                        tracing::info!("Transaction processing completed for request: 0x{:x}", request_id);
                    }
                    None => {
                        return Err(OffchainMarketMonitorErr::WebSocketErr(anyhow::anyhow!(
                            "Offchain order stream websocket exited, polling failed"
                        )));
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                return Ok(());
            }
        }
        }
    }

    // Bizim transaction'ımızı kontrol et ve lock durumunu da kontrol et
    async fn check_our_transaction_and_lock_status(
        provider: &Arc<P>,
        signer: &PrivateKeySigner,
        contract_address: Address,
        request_id: U256,
    ) -> Result<(bool, bool, Option<u64>), anyhow::Error> {
        // İlk önce request locked mı kontrol et
        let call = IBoundlessMarket::requestIsLockedCall {
            requestId: request_id
        };

        let call_request = alloy::rpc::types::TransactionRequest::default()
            .to(contract_address)
            .input(call.abi_encode().into());

        let result = provider.call(call_request).await
            .context("Failed to call requestIsLocked")?;

        let is_locked = IBoundlessMarket::requestIsLockedCall::abi_decode_returns(&result)
            .context("Failed to decode requestIsLocked result")?;

        tracing::debug!("Request 0x{:x} lock status: {}", request_id, is_locked);

        if !is_locked {
            // Request locked değil - bizim transaction da başarısız demek
            return Ok((false, false, None));
        }

        // Request locked - ama bizim transaction mı başkasının mı kontrol et
        // Son birkaç bloktaki bizim adresimizden gelen transaction'ları kontrol et
        let current_block = provider.get_block_number().await
            .context("Failed to get current block number")?;

        // Son 50 blok içinde bizim transaction'ımızı ara
        let start_block = current_block.saturating_sub(50);

        for block_num in start_block..=current_block {
            if let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).await {
                for tx_hash in block.transactions.hashes() {
                    let tx_hash_fixed = TxHash::from_slice(tx_hash.as_slice());

                    if let Ok(Some(tx)) = provider.get_transaction_by_hash(tx_hash_fixed).await {
                        // Transaction receipt'i al
                        if let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash_fixed).await {
                            let tx_from = receipt.from;
                            let tx_to = tx.to();

                            // Bizim adresimizden mi ve contract'a mı gönderilmiş
                            if tx_from == signer.address() && tx_to == Some(contract_address) {
                                if receipt.status() {
                                    let block_number = receipt.block_number.unwrap_or(block_num);
                                    tracing::debug!("Found our successful transaction: 0x{} at block: {}", tx_hash_fixed, block_number);
                                    return Ok((true, true, Some(block_number)));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Bizim başarılı transaction bulamadık ama request locked - başkası yapmış
        Ok((false, true, None))
    }

    async fn send_raw_transaction(
        order_data: &boundless_market::order_stream_client::OrderData,
        signer: &PrivateKeySigner,
        contract_address: Address,
        config: &MonitorConfig,
        prover_addr: Address,
        provider: Arc<P>,
    ) -> Result<u64, anyhow::Error> {
        let request_id = order_data.order.request.id;
        tracing::info!("🚀 Oregon: Forwarding order 0x{:x} to Virginia", request_id);

        // Virginia server endpoint
        let virginia_endpoint = "http://54.81.139.224:8080/api/process-order";

        // Order data'yı serialize et
        let payload = serde_json::to_value(order_data)
            .context("Failed to serialize order data")?;

        let forward_start = Instant::now();

        let client = reqwest::Client::new();
        let response = client
            .post(virginia_endpoint)
            .header("Content-Type", "application/json")
            .header("Connection", "keep-alive")
            .json(&payload)
            .send()
            .await
            .context("Failed to forward order to Virginia")?;

        let forward_elapsed = forward_start.elapsed();
        tracing::info!("⚡ Virginia Forward Time: {:.2}ms for request 0x{:x}",
             forward_elapsed.as_secs_f64() * 1000.0, request_id);

        // Status'u önce al
        let status = response.status();

        // Sonra response'u consume et
        let result: serde_json::Value = response.json().await
            .context("Failed to parse Virginia response")?;

        if status.is_success() && result.get("success").and_then(|v| v.as_bool()).unwrap_or(false) {
            let lock_block = result.get("lock_block").and_then(|v| v.as_u64()).unwrap_or(0);
            tracing::info!("✅ Virginia processed order 0x{:x}, lock_block: {}", request_id, lock_block);
            Ok(lock_block)
        } else {
            let error_msg = result.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
            tracing::error!("❌ Virginia processing failed for 0x{:x}: {}", request_id, error_msg);
            Err(anyhow::anyhow!("Virginia processing failed: {}", error_msg))
        }
    }

    async fn wait_for_transaction_receipt(
        provider: Arc<P>,
        tx_hash: TxHash,
    ) -> Result<alloy::rpc::types::TransactionReceipt, anyhow::Error> {
        tracing::info!("İşlem onayını bekliyor: 0x{}", hex::encode(tx_hash.as_slice()));

        const RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > RECEIPT_TIMEOUT {
                return Err(anyhow::anyhow!(
                    "Transaction 0x{} timeout after {} seconds",
                    hex::encode(tx_hash.as_slice()),
                    RECEIPT_TIMEOUT.as_secs()
                ));
            }

            match provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    let elapsed = start_time.elapsed();
                    tracing::info!("İşlem 0x{} başarıyla onaylandı ({:.1}s sonra)",
                                 hex::encode(tx_hash.as_slice()), elapsed.as_secs_f64());
                    return Ok(receipt);
                }
                Ok(None) => {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
                Err(e) => {
                    tracing::debug!("Error getting transaction receipt: {:?}, retrying...", e);
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            }
        }
    }



}



impl<P> RetryTask for OffchainMarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = OffchainMarketMonitorErr;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let client = self.client.clone();
        let signer = self.signer.clone();
        let prover_addr = self.prover_addr;
        let provider = self.provider.clone();
        let config = self.config.clone();

        Box::pin(async move {
            tracing::info!("Starting up offchain market monitor");
            Self::monitor_orders(client, signer, cancel_token, prover_addr, provider, config)
                .await
                .map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}