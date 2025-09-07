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
use std::fs;
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
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio::sync::RwLock;
use sqlx::{SqlitePool, Row};

#[derive(Error)]
pub enum OffchainMarketMonitorErr {
    #[error("Server error: {0:?}")]
    ServerErr(anyhow::Error),

    #[error("Websocket error: {0:?}")]
    WebSocketErr(anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} Database error: {0:?}", code = self.code())]
    DatabaseErr(#[from] sqlx::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl_coded_debug!(OffchainMarketMonitorErr);

impl CodedError for OffchainMarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            OffchainMarketMonitorErr::ServerErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::WebSocketErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::ReceiverDropped => "[B-OMM-002]",
            OffchainMarketMonitorErr::DatabaseErr(_) => "[B-OMM-003]",
            OffchainMarketMonitorErr::UnexpectedErr(_) => "[B-OMM-500]",
        }
    }
}

// Global cache'ler
static CACHED_CHAIN_ID: AtomicU64 = AtomicU64::new(0);
static CURRENT_NONCE: AtomicU64 = AtomicU64::new(0);
static IS_WAITING_FOR_COMMITTED_ORDERS: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
struct OptimizedHttpClient {
    client: reqwest::Client,
    rpc_url: String,
}

impl OptimizedHttpClient {
    fn new(rpc_url: String) -> Self {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(30)
            .pool_idle_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(1))
            .connect_timeout(Duration::from_millis(500))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true)
            .build()
            .expect("Failed to create HTTP client");

        Self { client, rpc_url }
    }

    async fn send_raw_transaction(&self, tx_encoded: &[u8]) -> Result<serde_json::Value> {
        let request_body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(tx_encoded))],
            "id": 1
        });

        let response = self.client
            .post(&self.rpc_url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .context("Failed to send raw transaction")?;

        let result: serde_json::Value = response.json().await
            .context("Failed to parse raw transaction response")?;

        Ok(result)
    }
}

#[derive(Clone)]
struct CachedConfig {
    pub listen_port: u16,
    pub rust_api_url: String,
    pub allowed_requestors: Option<HashSet<Address>>,
    pub allowed_backup_requestors: Option<HashSet<Address>>, // ✅ Yeni backup requestor sistemi
    pub lockin_priority_gas: u64,
    pub min_allowed_lock_timeout_secs: u64,
    pub http_rpc_url: String,
}

// ✅ Locked order struct - DB için
#[derive(Debug, Clone)]
struct LockedOrder {
    pub id: i64,
    pub request_id: String, // 0x formatında
    pub tx_hash: String,
    pub lock_block: u64,
    pub is_sent: bool,
    pub created_at: DateTime<Utc>,
}

pub struct OffchainMarketMonitor<P> {
    signer: PrivateKeySigner,
    prover_addr: Address,
    provider: Arc<P>,
    market_addr: Address,
    http_client: OptimizedHttpClient,
    cached_config: Arc<RwLock<CachedConfig>>,
    config: ConfigLock,
    db_pool: Arc<SqlitePool>, // ✅ SQLite pool eklendi
}

impl<P> OffchainMarketMonitor<P> where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        signer: PrivateKeySigner,
        prover_addr: Address,
        provider: Arc<P>,
        config_lock: ConfigLock,
        market_addr: Address,
        db_pool: Arc<SqlitePool>,
    ) -> Self {

        // ✅ Config'i başlangıçta cache'le
        let cached_config = {
            let conf = config_lock.lock_all().context("Failed to read config during initialization").unwrap();
            CachedConfig {
                listen_port: conf.market.listen_port,
                rust_api_url: conf.market.rust_api_url.clone(),
                allowed_requestors: conf.market.allow_requestor_addresses.clone(),
                allowed_backup_requestors: conf.market.allowed_backup_requestors.clone(), // ✅ Yeni field
                lockin_priority_gas: conf.market.lockin_priority_gas.unwrap_or(5000000),
                min_allowed_lock_timeout_secs: conf.market.min_lock_out_time * 60,
                http_rpc_url: conf.market.my_rpc_url.clone(),
            }
        };

        let http_client = OptimizedHttpClient::new(cached_config.http_rpc_url.clone());

        Self {
            signer,
            prover_addr,
            provider,
            market_addr,
            http_client,
            cached_config: Arc::new(RwLock::new(cached_config)),
            config: config_lock,
            db_pool,
        }
    }

    pub async fn init_database() -> Result<SqlitePool, OffchainMarketMonitorErr> {
        // PM2 working directory'den bağımsız mutlak yol
        let db_path = "/home/get-oflito/crates/broker/data/locked_orders.db";
        let db_dir = "/home/get-oflito/crates/broker/data";

        // Dizini oluştur
        if let Err(e) = fs::create_dir_all(db_dir) {
            tracing::error!("Failed to create database directory {}: {}", db_dir, e);
            return Err(OffchainMarketMonitorErr::DatabaseErr(
                sqlx::Error::Io(e.into())
            ));
        }

        let connection_string = format!("sqlite:{}", db_path);
        let pool = SqlitePool::connect(&connection_string).await.map_err(|e| {
            tracing::error!("Failed to connect to database: {}", e);
            OffchainMarketMonitorErr::DatabaseErr(e)
        })?;

        // Tablo oluştur
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS locked_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL UNIQUE,
            tx_hash TEXT NOT NULL,
            lock_block INTEGER NOT NULL,
            is_sent BOOLEAN NOT NULL DEFAULT FALSE,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        "#
        )
            .execute(&pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create table: {}", e);
                OffchainMarketMonitorErr::DatabaseErr(e)
            })?;

        tracing::info!("Database initialized successfully at: {}", db_path);
        Ok(pool)
    }

    // ✅ DB'ye locked order ekle
    async fn insert_locked_order(
        pool: &SqlitePool,
        request_id: &str,
        tx_hash: &str,
        lock_block: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO locked_orders (request_id, tx_hash, lock_block, is_sent) VALUES (?, ?, ?, FALSE)"
        )
            .bind(request_id)
            .bind(tx_hash)
            .bind(lock_block as i64)
            .execute(pool)
            .await?;

        tracing::info!("💾 Locked order saved to DB: {}", request_id);
        Ok(())
    }

    // ✅ DB'den unsent order'ları al
    async fn get_unsent_orders(pool: &SqlitePool) -> Result<Vec<LockedOrder>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, request_id, tx_hash, lock_block, is_sent, created_at FROM locked_orders WHERE is_sent = FALSE ORDER BY created_at ASC"
        )
            .fetch_all(pool)
            .await?;

        let mut orders = Vec::new();
        for row in rows {
            let created_at_str: String = row.get("created_at");
            let created_at = DateTime::parse_from_str(&created_at_str, "%Y-%m-%d %H:%M:%S")
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            orders.push(LockedOrder {
                id: row.get("id"),
                request_id: row.get("request_id"),
                tx_hash: row.get("tx_hash"),
                lock_block: row.get::<i64, _>("lock_block") as u64,
                is_sent: row.get("is_sent"),
                created_at,
            });
        }

        Ok(orders)
    }

    // ✅ Order'ı sent olarak işaretle
    async fn mark_order_as_sent(pool: &SqlitePool, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE locked_orders SET is_sent = TRUE WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;

        tracing::info!("✅ Order marked as sent: ID {}", id);
        Ok(())
    }

    // ✅ Unsent order sayısını al
    async fn count_unsent_orders(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM locked_orders WHERE is_sent = FALSE")
            .fetch_one(pool)
            .await?;

        Ok(row.get("count"))
    }

    async fn update_cached_config(config: &ConfigLock, cached_config: Arc<RwLock<CachedConfig>>) -> Result<()> {
        let updated_config = {
            let conf = config.lock_all().context("Failed to read config during update")?;
            let config_data = CachedConfig {
                listen_port: conf.market.listen_port,
                rust_api_url: conf.market.rust_api_url.clone(),
                allowed_requestors: conf.market.allow_requestor_addresses.clone(),
                allowed_backup_requestors: conf.market.allowed_backup_requestors.clone(), // ✅ Yeni field
                lockin_priority_gas: conf.market.lockin_priority_gas.unwrap_or(5000000),
                min_allowed_lock_timeout_secs: conf.market.min_lock_out_time * 60,
                http_rpc_url: conf.market.my_rpc_url.clone(),
            };

            let allowed_requestors_exists = conf.market.allow_requestor_addresses.is_some();
            let allowed_backup_exists = conf.market.allowed_backup_requestors.is_some();
            let rpc_url = conf.market.my_rpc_url.clone();
            let priority_gas = conf.market.lockin_priority_gas.unwrap_or(5000000);

            drop(conf);

            tracing::info!("📖📖📖 CONFIG GÜNCELLENDI 📖📖📖");
            tracing::info!("   - Allowed requestors: {:?}", allowed_requestors_exists);
            tracing::info!("   - Allowed backup requestors: {:?}", allowed_backup_exists);
            tracing::info!("   - RPC URL: {}", rpc_url);
            tracing::info!("   - Priority gas: {:?}", priority_gas);

            config_data
        };

        let mut cached = cached_config.write().await;
        *cached = updated_config;
        drop(cached);

        Ok(())
    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }

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

    // ✅ Enhanced committed orders polling with backup system
    async fn start_committed_orders_polling(
        rust_api_url: String,
        db_pool: Arc<SqlitePool>,
    ) -> Result<(), anyhow::Error> {
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
        let db_pool_clone = db_pool.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3 * 10));

            loop {
                interval.tick().await;

                if !IS_WAITING_FOR_COMMITTED_ORDERS.load(Ordering::Relaxed) {
                    continue;
                }

                tracing::info!("🔄 Checking committed orders status...");

                match Self::check_committed_orders(&rust_api_url_clone).await {
                    Ok(committed_count) => {
                        if committed_count == 0 {
                            tracing::info!("🎯 Committed orders = 0! Processing unsent orders...");

                            // ✅ DB'den unsent order'ları al ve sırayla gönder
                            match Self::process_unsent_orders(&rust_api_url_clone, &db_pool_clone).await {
                                Ok(has_more) => {
                                    if !has_more {
                                        // Eğer daha gönderilecek order yoksa normal monitoring'e geç
                                        tracing::info!("🧹 All orders processed. Resuming normal order monitoring...");
                                        IS_WAITING_FOR_COMMITTED_ORDERS.store(false, Ordering::Relaxed);
                                    } else {
                                        // Daha gönderilecek order var, committed polling devam etsin
                                        tracing::info!("📋 More orders to process. Staying in committed polling mode.");
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("❌ Error processing unsent orders: {}", e);
                                }
                            }
                        } else if committed_count > 0 {
                            tracing::info!("⏳ Still {} committed orders. Waiting...", committed_count);
                        } else {
                            tracing::warn!("⚠️ Error getting committed orders count. Retrying in 30sec...");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("⚠️ Failed to check committed orders: {}. Retrying in 30sec...", e);
                    }
                }
            }
        });

        Ok(())
    }

    // ✅ Unsent order'ları sırayla işle
    async fn process_unsent_orders(
        rust_api_url: &str,
        db_pool: &SqlitePool,
    ) -> Result<bool, anyhow::Error> {
        let unsent_orders = Self::get_unsent_orders(db_pool).await
            .context("Failed to get unsent orders")?;

        if unsent_orders.is_empty() {
            tracing::info!("✅ No unsent orders in database");
            return Ok(false);
        }

        // İlk unsent order'ı al ve gönder
        let first_order = &unsent_orders[0];
        tracing::info!("📤 Sending order to Rust API: {}", first_order.request_id);

        let success = Self::send_to_rust_api(
            rust_api_url,
            first_order.tx_hash.clone(),
            first_order.lock_block,
        ).await?;

        if success {
            // Order'ı sent olarak işaretle
            Self::mark_order_as_sent(db_pool, first_order.id).await
                .context("Failed to mark order as sent")?;

            tracing::info!("✅ Order {} sent successfully and marked in DB", first_order.request_id);
        } else {
            tracing::error!("❌ Failed to send order {}", first_order.request_id);
        }

        // Hala gönderilmemiş order var mı kontrol et
        let remaining_count = Self::count_unsent_orders(db_pool).await
            .context("Failed to count unsent orders")?;

        Ok(remaining_count > 0)
    }

    pub async fn monitor_orders(
        signer: PrivateKeySigner,
        cancel_token: CancellationToken,
        prover_addr: Address,
        provider: Arc<P>,
        market_addr: Address,
        http_client: OptimizedHttpClient,
        cached_config: Arc<RwLock<CachedConfig>>,
        config: ConfigLock,
        db_pool: Arc<SqlitePool>, // ✅ DB pool eklendi
    ) -> Result<(), OffchainMarketMonitorErr> {

        let chain_id = 8453u64;
        CACHED_CHAIN_ID.store(chain_id, Ordering::Relaxed);
        let initial_nonce = provider.get_transaction_count(signer.address()).pending().await.context("Failed to get transaction count")?;
        CURRENT_NONCE.store(initial_nonce, Ordering::Relaxed);

        let rust_api_url = {
            let config_read = cached_config.read().await;
            config_read.rust_api_url.clone()
        };

        // ✅ Enhanced polling with DB integration
        Self::start_committed_orders_polling(rust_api_url, db_pool.clone()).await
            .map_err(|e| OffchainMarketMonitorErr::UnexpectedErr(e))?;

        let listen_port = {
            let config_read = cached_config.read().await;
            config_read.listen_port
        };

        let addr: SocketAddr = format!("0.0.0.0:{}", listen_port).parse()
            .map_err(|e| OffchainMarketMonitorErr::UnexpectedErr(anyhow::anyhow!("Invalid address: {}", e)))?;

        let listener = TcpListener::bind(addr).await
            .map_err(|e| OffchainMarketMonitorErr::ServerErr(anyhow::anyhow!("Failed to bind: {}", e)))?;

        tracing::info!("🎧 WebSocket server started on port {}", listen_port);

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, client_addr)) => {
                            tracing::info!("📞 New connection from: {}", client_addr);

                            let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                                Ok(ws) => {
                                    tracing::info!("✅ WebSocket handshake successful with {}", client_addr);
                                    ws
                                }
                                Err(e) => {
                                    tracing::error!("❌ WebSocket handshake failed with {}: {}", client_addr, e);
                                    continue;
                                }
                            };

                            let signer_clone = signer.clone();
                            let provider_clone = provider.clone();
                            let cancel_token_clone = cancel_token.clone();
                            let http_client_clone = http_client.clone();
                            let cached_config_clone = cached_config.clone();
                            let config_clone = config.clone();
                            let db_pool_clone = db_pool.clone(); // ✅ DB pool clone

                            tokio::spawn(async move {
                                tracing::info!("🚀 Starting connection handler for {}", client_addr);

                                tokio::select! {
                                    _ = Self::handle_websocket_connection(
                                        ws_stream,
                                        &signer_clone,
                                        &provider_clone,
                                        market_addr,
                                        prover_addr,
                                        &http_client_clone,
                                        cached_config_clone,
                                        config_clone,
                                        db_pool_clone, // ✅ DB pool geçir
                                    ) => {
                                        tracing::info!("📴 Connection handler finished for {}", client_addr);
                                    }
                                    _ = cancel_token_clone.cancelled() => {
                                        tracing::info!("🛑 Connection handler cancelled for {}", client_addr);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("❌ Failed to accept connection: {}", e);
                            continue;
                        }
                    }
                }

                _ = cancel_token.cancelled() => {
                    tracing::info!("🛑 Server shutdown requested");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_websocket_connection(
        mut ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        signer: &PrivateKeySigner,
        provider: &Arc<P>,
        contract_address: Address,
        prover_addr: Address,
        http_client: &OptimizedHttpClient,
        cached_config: Arc<RwLock<CachedConfig>>,
        config: ConfigLock,
        db_pool: Arc<SqlitePool>, // ✅ DB pool eklendi
    ) {
        tracing::info!("Starting persistent WebSocket message handler");

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let response = Self::process_order_ws(
                        text,
                        signer,
                        provider,
                        contract_address,
                        prover_addr,
                        http_client,
                        cached_config.clone(),
                        config.clone(),
                        db_pool.clone(), // ✅ DB pool geçir
                    ).await;

                    if let Err(e) = ws_stream.send(Message::Text(response)).await {
                        tracing::error!("Failed to send response: {}", e);
                        break;
                    }
                }
                Ok(Message::Close(_)) => {
                    tracing::info!("Client closed connection");
                    break;
                }
                Err(e) => {
                    tracing::error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        tracing::info!("WebSocket connection closed");
    }

    async fn process_order_ws(
        body: String,
        signer: &PrivateKeySigner,
        provider: &Arc<P>,
        contract_address: Address,
        prover_addr: Address,
        http_client: &OptimizedHttpClient,
        cached_config: Arc<RwLock<CachedConfig>>,
        config: ConfigLock,
        db_pool: Arc<SqlitePool>, // ✅ DB pool eklendi
    ) -> String {
        let order_data: boundless_market::order_stream_client::OrderData = match serde_json::from_str(&body) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("Failed to parse order data: {:?}", e);
                return r#"{"error":"Invalid JSON"}"#.to_string();
            }
        };

        let request_id = order_data.order.request.id;
        let client_addr = order_data.order.request.client_address();
        let request_id_hex = format!("0x{:x}", request_id);

        // ✅ Normal flow - waiting for committed orders olmadığında
        if !IS_WAITING_FOR_COMMITTED_ORDERS.load(Ordering::Relaxed) {
            // Normal allowed_requestors kontrolü
            {
                let config_read = cached_config.read().await;
                if let Some(ref allow_addresses) = config_read.allowed_requestors {
                    if !allow_addresses.contains(&client_addr) {
                        tracing::debug!("Client not in allowed requestors, skipping request: {}", request_id_hex);
                        return r#"{"error":"Client not allowed"}"#.to_string();
                    }
                }
            }

            // Normal processing
            return Self::process_normal_order(
                order_data,
                signer,
                provider,
                contract_address,
                prover_addr,
                http_client,
                cached_config,
                config,
            ).await;
        }

        // ✅ Backup system - committed orders beklerken
        // Backup requestor kontrolü
        let is_backup_requestor = {
            let config_read = cached_config.read().await;
            if let Some(ref backup_addresses) = config_read.allowed_backup_requestors {
                backup_addresses.contains(&client_addr)
            } else {
                false
            }
        };

        if !is_backup_requestor {
            tracing::debug!("Not a backup requestor during committed orders wait: {}", request_id_hex);
            return r#"{"status":"waiting_for_committed_orders"}"#.to_string();
        }

        // Backup order sayısını kontrol et (max 4)
        match Self::count_unsent_orders(&db_pool).await {
            Ok(count) if count >= 4 => {
                tracing::info!("🔒 Maximum backup orders (4) reached, skipping: {}", request_id_hex);
                return r#"{"status":"backup_limit_reached"}"#.to_string();
            }
            Ok(count) => {
                tracing::info!("📦 Processing backup order ({}/4): {}", count + 1, request_id_hex);
            }
            Err(e) => {
                tracing::error!("❌ Failed to check backup order count: {}", e);
                return r#"{"error":"Database error"}"#.to_string();
            }
        }

        // ✅ Lock timeout kontrolü
        {
            let config_read = cached_config.read().await;
            if (order_data.order.request.offer.lockTimeout as u64) < config_read.min_allowed_lock_timeout_secs {
                tracing::info!(
                    "Skipping backup order {}: Lock Timeout ({} seconds) is less than minimum required ({} seconds).",
                    request_id_hex,
                    order_data.order.request.offer.lockTimeout,
                    config_read.min_allowed_lock_timeout_secs
                );
                return r#"{"error":"Lock timeout too short"}"#.to_string();
            }
        }

        // Backup order processing
        let order_received_time = Instant::now();
        tracing::info!("🔄 BACKUP ORDER RECEIVED - Request ID: {} at {}", request_id_hex, Self::format_time(chrono::Utc::now()));

        match Self::send_backup_transaction(
            &order_data,
            signer,
            contract_address,
            prover_addr,
            provider.clone(),
            http_client,
            cached_config,
            config,
            db_pool,
        ).await {
            Ok(lock_block) => {
                tracing::info!("✅ BACKUP LOCK SUCCESS! Request: {}, Block: {}", request_id_hex, lock_block);
                format!(r#"{{"status":"backup_success","lock_block":{}}}"#, lock_block)
            }
            Err(err) => {
                tracing::error!("❌ Backup transaction error for request: {}, error: {}", request_id_hex, err);
                return r#"{"error":"Backup transaction failed"}"#.to_string();
            }
        }
    }

    // ✅ Normal order processing
    async fn process_normal_order(
        order_data: boundless_market::order_stream_client::OrderData,
        signer: &PrivateKeySigner,
        provider: &Arc<P>,
        contract_address: Address,
        prover_addr: Address,
        http_client: &OptimizedHttpClient,
        cached_config: Arc<RwLock<CachedConfig>>,
        config: ConfigLock,
    ) -> String {
        let request_id = order_data.order.request.id;
        let request_id_hex = format!("0x{:x}", request_id);

        // Lock timeout kontrolü
        {
            let config_read = cached_config.read().await;
            if (order_data.order.request.offer.lockTimeout as u64) < config_read.min_allowed_lock_timeout_secs {
                tracing::info!(
                    "Skipping order {}: Lock Timeout ({} seconds) is less than minimum required ({} seconds).",
                    request_id_hex,
                    order_data.order.request.offer.lockTimeout,
                    config_read.min_allowed_lock_timeout_secs
                );
                return r#"{"error":"Lock timeout too short"}"#.to_string();
            }
        }

        let order_received_time = Instant::now();
        tracing::info!("📨 ORDER RECEIVED - Request ID: {} at {}", request_id_hex, Self::format_time(chrono::Utc::now()));

        let pre_send_elapsed = order_received_time.elapsed();
        tracing::info!("PRE-SEND PROCESSING TIME: {:.2}ms for request {}",
            pre_send_elapsed.as_secs_f64() * 1000.0, request_id_hex);

        match Self::send_raw_transaction(
            &order_data,
            signer,
            contract_address,
            prover_addr,
            provider.clone(),
            http_client,
            cached_config,
            config,
        ).await {
            Ok(lock_block) => {
                tracing::info!("✅ LOCK SUCCESS! Request: {}, Block: {}", request_id_hex, lock_block);

                let lock_timestamp = match provider
                    .get_block_by_number(lock_block.into())
                    .await
                {
                    Ok(Some(block)) => block.header.timestamp,
                    Ok(None) => {
                        tracing::error!("CRITICAL: Block {} not found after successful lock!", lock_block);
                        return r#"{"error":"Block not found"}"#.to_string();
                    }
                    Err(e) => {
                        tracing::error!("CRITICAL: Failed to get block {} after successful lock: {:?}", lock_block, e);
                        return r#"{"error":"Failed to get block"}"#.to_string();
                    }
                };

                let lock_price = match order_data.order.request.offer.price_at(lock_timestamp) {
                    Ok(price) => price,
                    Err(e) => {
                        tracing::error!("CRITICAL: Failed to calculate lock price after successful lock: {:?}", e);
                        return r#"{"error":"Failed to calculate price"}"#.to_string();
                    }
                };

                tracing::info!("Lock successful for request {}, price: {}, block: {}",
                     request_id_hex, lock_price, lock_block);

                format!(r#"{{"status":"success","lock_block":{}}}"#, lock_block)
            }
            Err(err) => {
                tracing::error!("❌ Transaction error for request: {}, error: {}", request_id_hex, err);
                return r#"{"error":"Transaction failed"}"#.to_string();
            }
        }
    }

    // ✅ Backup transaction processing
    async fn send_backup_transaction(
        order_data: &boundless_market::order_stream_client::OrderData,
        signer: &PrivateKeySigner,
        contract_address: Address,
        prover_addr: Address,
        provider: Arc<P>,
        http_client: &OptimizedHttpClient,
        cached_config: Arc<RwLock<CachedConfig>>,
        config: ConfigLock,
        db_pool: Arc<SqlitePool>,
    ) -> Result<u64, anyhow::Error> {
        let request_id_hex = format!("0x{:x}", order_data.order.request.id);

        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);
        let current_nonce = CURRENT_NONCE.load(Ordering::Relaxed);
        CURRENT_NONCE.store(current_nonce + 1, Ordering::Relaxed);

        let lock_call = IBoundlessMarket::lockRequestCall {
            request: order_data.order.request.clone(),
            clientSignature: order_data.order.signature.as_bytes().into(),
        };

        let lock_calldata = lock_call.abi_encode();

        let (max_priority_fee_per_gas, _) = {
            let config_read = cached_config.read().await;
            (config_read.lockin_priority_gas.into(), config_read.rust_api_url.clone())
        };

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

        tracing::info!("🚀 SENDING BACKUP TRANSACTION...");
        let send_start = Instant::now();
        let result = http_client.send_raw_transaction(&tx_encoded).await?;
        let send_duration = send_start.elapsed();
        tracing::info!("⚡ Backup TX send duration: {:?}", send_duration);

        if let Some(error) = result.get("error") {
            let error_message = error.to_string().to_lowercase();

            if error_message.contains("nonce") {
                tracing::error!("Backup nonce error: {}", error);

                let fresh_nonce = provider
                    .get_transaction_count(signer.address())
                    .pending()
                    .await
                    .context("Failed to get fresh transaction count")?;

                CURRENT_NONCE.store(fresh_nonce, Ordering::Relaxed);
                tracing::info!("Backup nonce resynchronized from {} to {}", current_nonce, fresh_nonce);

                return Err(anyhow::anyhow!("Backup nonce error - resynchronized: {}", error));
            }

            let prev_nonce = current_nonce;
            CURRENT_NONCE.store(prev_nonce, Ordering::Relaxed);
            tracing::warn!("Backup transaction failed, rolled back nonce to: {}", prev_nonce);

            return Err(anyhow::anyhow!("Backup raw transaction failed: {}", error));
        }

        let tx_hash = result["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No transaction hash in backup response"))?
            .to_string();

        let tx_hash_parsed = tx_hash.parse()
            .context("Failed to parse backup transaction hash")?;

        tracing::info!("🔄 Backup raw transaction hash: {}", tx_hash);

        let tx_receipt = Self::wait_for_transaction_receipt(provider.clone(), tx_hash_parsed)
            .await
            .context("Failed to get backup transaction receipt")?;

        if !tx_receipt.status() {
            tracing::warn!("🔄 Backup transaction {} REVERTED.", tx_hash);
            return Err(anyhow::anyhow!("Backup transaction reverted on chain"));
        }

        let lock_block = tx_receipt.block_number
            .ok_or_else(|| anyhow::anyhow!("No block number in backup receipt"))?;

        tracing::info!("✅ Backup transaction {} confirmed successfully. Block: {}", tx_hash, lock_block);

        // ✅ Backup order'ı DB'ye kaydet (is_sent = false)
        if let Err(e) = Self::insert_locked_order(&db_pool, &request_id_hex, &tx_hash, lock_block).await {
            tracing::error!("❌ Failed to save backup order to DB: {}", e);
            // DB error'u olsa bile transaction başarılı, devam et
        }

        tracing::info!("💾 Backup order saved to database: {}", request_id_hex);

        Ok(lock_block)
    }

    async fn send_raw_transaction(
        order_data: &boundless_market::order_stream_client::OrderData,
        signer: &PrivateKeySigner,
        contract_address: Address,
        prover_addr: Address,
        provider: Arc<P>,
        http_client: &OptimizedHttpClient,
        cached_config: Arc<RwLock<CachedConfig>>,
        config: ConfigLock,
    ) -> Result<u64, anyhow::Error> {
        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);
        let current_nonce = CURRENT_NONCE.load(Ordering::Relaxed);
        CURRENT_NONCE.store(current_nonce + 1, Ordering::Relaxed);

        let lock_call = IBoundlessMarket::lockRequestCall {
            request: order_data.order.request.clone(),
            clientSignature: order_data.order.signature.as_bytes().into(),
        };

        let lock_calldata = lock_call.abi_encode();

        let (max_priority_fee_per_gas, rust_api_url) = {
            let config_read = cached_config.read().await;
            (config_read.lockin_priority_gas.into(), config_read.rust_api_url.clone())
        };

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

        tracing::info!("------- SENDING NOW ------");
        let send_start = Instant::now();
        let result = http_client.send_raw_transaction(&tx_encoded).await?;
        let send_duration = send_start.elapsed();
        tracing::info!("🚀 Raw TX send duration: {:?}", send_duration);

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
            tracing::warn!("\x1b[95mİşlem {} REVERT oldu. Lock alınamadı.\x1b[0m", tx_hash);
            return Err(anyhow::anyhow!("Transaction reverted on chain"));
        }

        let lock_block = tx_receipt.block_number
            .ok_or_else(|| anyhow::anyhow!("No block number in receipt"))?;

        tracing::info!("\x1b[32mİşlem {} başarıyla onaylandı. Lock alındı. Block: {}\x1b[0m", tx_hash, lock_block);

        // Rust API'ye lock verilerini gönder
        tracing::info!("🦀 Rust API'ye lock verilerini gönderme başlatılıyor...");

        match Self::send_to_rust_api(&rust_api_url, tx_hash.clone(), lock_block).await {
            Ok(true) => {
                tracing::info!("✅ Rust API'ye başarıyla veri gönderildi. Artık committed orders polling'e geçiliyor...");

                if let Err(e) = Self::update_cached_config(&config, cached_config.clone()).await {
                    tracing::error!("❌ Failed to update cached_config: {:?}", e);
                }

                IS_WAITING_FOR_COMMITTED_ORDERS.store(true, Ordering::Relaxed);
                tracing::info!("🔄 Order monitoring durduruldu. Her 30 saniyede committed orders kontrol edilecek.");
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
        let market_addr = self.market_addr;
        let cached_config = self.cached_config.clone();
        let config = self.config.clone();
        let http_client = self.http_client.clone();
        let db_pool = self.db_pool.clone(); // ✅ DB pool clone

        Box::pin(async move {
            tracing::info!("Starting up offchain market monitor with backup system");
            Self::monitor_orders(
                signer,
                cancel_token,
                prover_addr,
                provider,
                market_addr,
                http_client,
                cached_config,
                config,
                db_pool, // ✅ DB pool geçir
            )
                .await
                .map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}