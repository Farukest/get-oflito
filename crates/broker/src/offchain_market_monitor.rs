use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use alloy::network::Ethereum;
use alloy::primitives::{Address, U256, TxHash, FixedBytes};
use alloy::providers::Provider;
use alloy::signers::{local::PrivateKeySigner, Signer};
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
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

#[derive(Error)]
pub enum OffchainMarketMonitorErr {
    #[error("Server error: {0:?}")]
    ServerErr(anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl_coded_debug!(OffchainMarketMonitorErr);

impl CodedError for OffchainMarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            OffchainMarketMonitorErr::ServerErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::ReceiverDropped => "[B-OMM-002]",
            OffchainMarketMonitorErr::UnexpectedErr(_) => "[B-OMM-500]",
        }
    }
}

// Global cache'ler
static CACHED_CHAIN_ID: AtomicU64 = AtomicU64::new(0);
static CURRENT_NONCE: AtomicU64 = AtomicU64::new(0);
static IS_WAITING_FOR_COMMITTED_ORDERS: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct MonitorConfig {
    pub listen_port: u16,
    pub rust_api_url: String,
    pub allowed_requestors: Option<HashSet<Address>>,
    pub lockin_priority_gas: u64,
    pub min_allowed_lock_timeout_secs: u64,
    pub http_rpc_url: String,
}

pub struct OffchainMarketMonitor<P> {
    signer: PrivateKeySigner,
    prover_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
}

impl<P> OffchainMarketMonitor<P> where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        signer: PrivateKeySigner,
        prover_addr: Address,
        provider: Arc<P>,
        config_lock: ConfigLock,
    ) -> Self {
        Self {
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

        let rust_api_url_clone = rust_api_url.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3 * 60)); // 3 dakika

            loop {
                interval.tick().await;

                if !IS_WAITING_FOR_COMMITTED_ORDERS.load(Ordering::Relaxed) {
                    continue;
                }

                tracing::info!("🔄 Checking committed orders status...");

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

    pub async fn monitor_orders(
        signer: PrivateKeySigner,
        cancel_token: CancellationToken,
        prover_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        contract_address: Address,
    ) -> Result<(), OffchainMarketMonitorErr> {
        // Config'i başta oku
        let monitor_config = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            MonitorConfig {
                listen_port: locked_conf.market.listen_port,
                rust_api_url: locked_conf.market.rust_api_url.clone(),
                allowed_requestors: locked_conf.market.allow_requestor_addresses.clone(),
                lockin_priority_gas: locked_conf.market.lockin_priority_gas.unwrap_or(5000000),
                min_allowed_lock_timeout_secs: locked_conf.market.min_lock_out_time * 60,
                http_rpc_url: locked_conf.market.my_rpc_url.clone(),
            }
        };

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

        // HTTP server'ı başlat
        let addr: SocketAddr = format!("0.0.0.0:{}", monitor_config.listen_port).parse()
            .map_err(|e| OffchainMarketMonitorErr::UnexpectedErr(anyhow::anyhow!("Invalid address: {}", e)))?;

        let listener = TcpListener::bind(addr).await
            .map_err(|e| OffchainMarketMonitorErr::ServerErr(anyhow::anyhow!("Failed to bind: {}", e)))?;

        tracing::info!("🎧 Offchain market monitor started on port {}", monitor_config.listen_port);

        let mut is_processing = false;

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((mut socket, _)) => {
                            let signer = signer.clone();
                            let provider = provider.clone();
                            let config = monitor_config.clone();
                            let is_processing_ref = &mut is_processing;

                            tokio::spawn(async move {
                                let mut buffer = [0; 4096];
                                match socket.read(&mut buffer).await {
                                    Ok(n) if n > 0 => {
                                        let request = String::from_utf8_lossy(&buffer[..n]);
                                        let response = Self::handle_http_request(
                                            &request,
                                            &signer,
                                            &provider,
                                            &config,
                                            contract_address,
                                            prover_addr,
                                            is_processing_ref,
                                        ).await;

                                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                                            tracing::warn!("Failed to write response: {}", e);
                                        }
                                    }
                                    Ok(_) => {
                                        tracing::debug!("Empty request received");
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to read from socket: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("Offchain market monitor cancelled");
                    return Ok(());
                }
            }
        }
    }

    // HTTP isteğini işle
    async fn handle_http_request(
        request: &str,
        signer: &PrivateKeySigner,
        provider: &Arc<P>,
        config: &MonitorConfig,
        contract_address: Address,
        prover_addr: Address,
        is_processing: &mut bool,
    ) -> String {
        // Basit HTTP parsing
        let lines: Vec<&str> = request.lines().collect();

        if let Some(first_line) = lines.first() {
            let parts: Vec<&str> = first_line.split_whitespace().collect();

            if parts.len() >= 3 && parts[0] == "POST" && parts[1] == "/api/order" {
                // POST body'sini bul
                if let Some(empty_line_index) = lines.iter().position(|&line| line.is_empty()) {
                    let body_lines = &lines[empty_line_index + 1..];
                    let body = body_lines.join("\n");

                    return Self::process_order(body, signer, provider, config, contract_address, prover_addr, is_processing).await;
                }
            }
        }

        // Health check
        if request.contains("GET /health") {
            return "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}\r\n".to_string();
        }

        "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Not Found\"}\r\n".to_string()
    }

    // Order'ı işle
    async fn process_order(
        body: String,
        signer: &PrivateKeySigner,
        provider: &Arc<P>,
        config: &MonitorConfig,
        contract_address: Address,
        prover_addr: Address,
        is_processing: &mut bool,
    ) -> String {
        // Eğer committed orders bekleme modundaysak order processing yapma
        if IS_WAITING_FOR_COMMITTED_ORDERS.load(Ordering::Relaxed) {
            return "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"waiting_for_committed_orders\"}\r\n".to_string();
        }

        if *is_processing {
            tracing::warn!("Zaten bir transaction işleniyor, yeni order beklemede...");
            return "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Already processing\"}\r\n".to_string();
        }

        // JSON parse et
        let order_json: serde_json::Value = match serde_json::from_str(&body) {
            Ok(json) => json,
            Err(_) => {
                return "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Invalid JSON\"}\r\n".to_string();
            }
        };

        // Order data'yı parse et
        let order_data = match Self::parse_order_from_json(&order_json) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("Failed to parse order data: {:?}", e);
                return "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Invalid order format\"}\r\n".to_string();
            }
        };

        *is_processing = true;

        let request_id = order_data.order.request.id;
        let client_addr = order_data.order.request.client_address();

        // İzin verilen adres kontrolü
        if let Some(ref allow_addresses) = config.allowed_requestors {
            if !allow_addresses.contains(&client_addr) {
                tracing::debug!("Client not in allowed requestors, skipping request: 0x{:x}", request_id);
                *is_processing = false;
                return "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Client not allowed\"}\r\n".to_string();
            }
        }

        // **ORDER ID ALINDI - TIMING BAŞLAT**
        let order_received_time = Instant::now();
        tracing::info!("ORDER RECEIVED - Request ID: 0x{:x} at {}", request_id, Self::format_time(chrono::Utc::now()));

        // Lock timeout kontrolü
        if (order_data.order.request.offer.lockTimeout as u64) < config.min_allowed_lock_timeout_secs {
            tracing::info!(
                "Skipping order {}: Lock Timeout ({} seconds) is less than minimum required ({} seconds).",
                order_data.order.request.id,
                order_data.order.request.offer.lockTimeout,
                config.min_allowed_lock_timeout_secs
            );
            *is_processing = false;
            return "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Lock timeout too short\"}\r\n".to_string();
        }

        // **RAW TRANSACTION GÖNDERİM ÖNCESİ SÜRE HESAPLA**
        let pre_send_elapsed = order_received_time.elapsed();
        tracing::info!("PRE-SEND PROCESSING TIME: {:.2}ms for request 0x{:x}",
            pre_send_elapsed.as_secs_f64() * 1000.0, request_id);

        // WebSocket ile raw transaction gönder
        match Self::send_raw_transaction(
            &order_data,
            signer,
            contract_address,
            config,
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
                        *is_processing = false;
                        return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Block not found\"}\r\n".to_string();
                    }
                    Err(e) => {
                        tracing::error!("CRITICAL: Failed to get block {} after successful lock: {:?}", lock_block, e);
                        *is_processing = false;
                        return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Failed to get block\"}\r\n".to_string();
                    }
                };

                // Lock price hesapla
                let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                    Ok(price) => price,
                    Err(e) => {
                        tracing::error!("CRITICAL: Failed to calculate lock price after successful lock: {:?}", e);
                        *is_processing = false;
                        return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Failed to calculate price\"}\r\n".to_string();
                    }
                };

                tracing::info!("Lock successful for request 0x{:x}, price: {}, block: {}",
                             request_id, lock_price, lock_block);
                *is_processing = false;
                return "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"success\",\"lock_block\":" + &lock_block.to_string() + "}\r\n".to_string();
            }
            Err(err) => {
                tracing::error!("Transaction error for request: 0x{:x}, error: {}", request_id, err);

                // Bizim tx kontrolü ve block numarası al
                match Self::check_our_transaction_and_lock_status(provider, signer, contract_address, request_id).await {
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
                                *is_processing = false;
                                return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Lock block not found\"}\r\n".to_string();
                            }
                            Err(e) => {
                                tracing::error!("CRITICAL: Failed to get lock block {}: {:?}", lock_block, e);
                                *is_processing = false;
                                return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Failed to get lock block\"}\r\n".to_string();
                            }
                        };

                        let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                            Ok(price) => price,
                            Err(e) => {
                                tracing::error!("CRITICAL: Failed to calculate lock price for block {}: {:?}", lock_block, e);
                                *is_processing = false;
                                return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Failed to calculate lock price\"}\r\n".to_string();
                            }
                        };

                        tracing::info!("Our transaction was successful, request 0x{:x} locked at block {} with price {}",
                                     request_id, lock_block, lock_price);
                        *is_processing = false;
                        return "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"success\",\"lock_block\":" + &lock_block.to_string() + "}\r\n".to_string();
                    }
                    Ok((false, true, _)) => {
                        // Başkası lock'ladı - skipped
                        tracing::info!("Request 0x{:x} locked by someone else - skipping", request_id);
                        *is_processing = false;
                        return "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Locked by someone else\"}\r\n".to_string();
                    }
                    Ok((_, false, _)) => {
                        // Lock yok - gerçekten başarısız
                        tracing::info!("Request 0x{:x} confirmed NOT locked", request_id);
                        *is_processing = false;
                        return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Transaction failed\"}\r\n".to_string();
                    }
                    Ok((true, true, None)) => {
                        // Bizim transaction başarılı VE request locked + block numarası gelmedi
                        tracing::info!("true, true, None : Request 0x{:x} confirmed NOT locked", request_id);
                        *is_processing = false;
                        return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"No lock block found\"}\r\n".to_string();
                    }
                    Err(check_err) => {
                        tracing::error!("Failed to check transaction and lock status for 0x{:x}: {:?}", request_id, check_err);
                        *is_processing = false;
                        return "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Status check failed\"}\r\n".to_string();
                    }
                }
            }
        }
    }

    // JSON'dan order data parse et
    fn parse_order_from_json(json: &serde_json::Value) -> Result<boundless_market::order_stream_client::OrderData, anyhow::Error> {
        // Bu fonksiyonu gerçek order data structure'a göre implement etmeniz gerekiyor
        // Şimdilik placeholder
        todo!("Implement JSON to OrderData parsing")
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
        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);
        let current_nonce = CURRENT_NONCE.load(Ordering::Relaxed);
        CURRENT_NONCE.store(current_nonce + 1, Ordering::Relaxed);

        let lock_call = IBoundlessMarket::lockRequestCall {
            request: order_data.order.request.clone(),
            clientSignature: order_data.order.signature.as_bytes().into(),
        };

        let lock_calldata = lock_call.abi_encode();

        let max_priority_fee_per_gas = config.lockin_priority_gas.into();
        let min_competitive_gas = 60_000_000u128;
        let base_fee = min_competitive_gas;
        let max_fee_per_gas = base_fee + max_priority_fee_per_gas;

        let tx = TxEip1559 {
            chain_id,
            nonce: current_nonce,
            gas_limit: 500_000u64,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(contract_address),
            value: U256::ZERO,
            input: lock_calldata.into(),
            access_list: Default::default(),
        };

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash(&signature_hash).await?;
        let tx_signed = tx.into_signed(signature);
        let tx_envelope: TxEnvelope = tx_signed.into();
        let tx_encoded = tx_envelope.encoded_2718();

        let expected_tx_hash = tx_envelope.tx_hash();

        // **HTTP İSTEK ZAMANINI ÖLÇME - Node.js'teki gibi**
        let http_request_start = Instant::now();

        // HTTP ile eth_sendRawTransaction - ESKİ PRIVATE TRANSACTION KODUNU DEĞİŞTİRİN
        let rclient = reqwest::Client::new();
        let response = rclient
            .post(&config.http_rpc_url)
            .header("Content-Type", "application/json")
            .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",  // SADECE BU DEĞİŞTİ!
            "params": [format!("0x{}", hex::encode(&tx_encoded))],  // VE BU DEĞİŞTİ!
            "id": 1
        }))
            .send()
            .await
            .context("Failed to send raw transaction request")?;

        let http_request_elapsed = http_request_start.elapsed();
        tracing::info!("HTTP REQUEST TIME (Base Sequencer Response Time): {:.2}ms for request 0x{:x}",
        http_request_elapsed.as_secs_f64() * 1000.0, order_data.order.request.id);

        let result: serde_json::Value = response.json().await
            .context("Failed to parse response JSON")?;

        if let Some(error) = result.get("error") {
            let error_message = error.to_string().to_lowercase();

            if error_message.contains("nonce") {
                tracing::error!("Nonce hatası: {}", error);

                let fresh_nonce = provider
                    .get_transaction_count(signer.address())
                    .pending()
                    .await
                    .context("Failed to get fresh transaction count")?;

                CURRENT_NONCE.store(fresh_nonce, Ordering::Relaxed);
                tracing::info!("Nonce resynchronized from {} to {}", current_nonce, fresh_nonce);

                return Err(anyhow::anyhow!("Nonce error - resynchronized: {}", error));
            }

            let prev_nonce = current_nonce;
            CURRENT_NONCE.store(prev_nonce, Ordering::Relaxed);
            tracing::warn!("Transaction failed, rolled back nonce to: {}", prev_nonce);

            return Err(anyhow::anyhow!("Raw transaction failed: {}", error));
        }

        let tx_hash = result["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No transaction hash in response"))?
            .to_string();

        let tx_hash_parsed = tx_hash.parse()
            .context("Failed to parse transaction hash")?;
        tracing::info!("Raw transaction hash: {}", tx_hash);

        let tx_receipt = Self::wait_for_transaction_receipt(provider.clone(), tx_hash_parsed)
            .await
            .context("Failed to get transaction receipt")?;

        if !tx_receipt.status() {
            tracing::warn!("İşlem {} REVERT oldu. Lock alınamadı.", tx_hash);
            return Err(anyhow::anyhow!("Transaction reverted on chain"));
        }

        let lock_block = tx_receipt.block_number
            .ok_or_else(|| anyhow::anyhow!("No block number in receipt"))?;

        tracing::info!("İşlem {} başarıyla onaylandı. Lock alındı. Block: {}", tx_hash, lock_block);

        // Rust API'ye lock verilerini gönder
        tracing::info!("🦀 Rust API'ye lock verilerini gönderme başlatılıyor...");

        match Self::send_to_rust_api(&config.rust_api_url, tx_hash.clone(), lock_block).await {
            Ok(true) => {
                tracing::info!("✅ Rust API'ye başarıyla veri gönderildi. Artık committed orders polling'e geçiliyor...");

                // Artık order monitoring'i durdur ve committed orders polling'i başlat
                IS_WAITING_FOR_COMMITTED_ORDERS.store(true, Ordering::Relaxed);
                tracing::info!("🔄 Order monitoring durduruldu. Her 3 dakikada committed orders kontrol edilecek.");
            }
            Ok(false) => {
                tracing::error!("❌ Rust API'ye veri gönderilemedi. Program sonlandırılıyor...");
                std::process::exit(0);
            }
            Err(e) => {
                tracing::error!("❌ Rust API communication failed: {:?}. Program sonlandırılıyor...", e);
                std::process::exit(0);
            }
        }

        Ok(lock_block)
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
        let signer = self.signer.clone();
        let prover_addr = self.prover_addr;
        let provider = self.provider.clone();
        let config = self.config.clone();

        Box::pin(async move {
            tracing::info!("Starting up offchain market monitor");
            Self::monitor_orders(signer, cancel_token, prover_addr, provider, config, /* contract_address burada gerekli */)
                .await
                .map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}