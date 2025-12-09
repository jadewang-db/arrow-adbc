# Databricks Rust ADBC Driver - Detailed Implementation Plan

**Document Version**: 1.0
**Created**: 2024-12-08
**Design Reference**: [databricks-rust-adbc-driver-design.md](./databricks-rust-adbc-driver-design.md)

---

## Table of Contents

1. [Sprint 1: Foundation & Core Infrastructure](#sprint-1-foundation--core-infrastructure)
2. [Sprint 2: Connection & Basic Statement Execution](#sprint-2-connection--basic-statement-execution)
3. [Sprint 3: External Links & Parallel Chunk Fetching](#sprint-3-external-links--parallel-chunk-fetching)
4. [Sprint 4: Metadata APIs & execute_update](#sprint-4-metadata-apis--execute_update)
5. [Sprint 5: Testing, Polish & Release Preparation](#sprint-5-testing-polish--release-preparation)

---

# Sprint 1: Foundation & Core Infrastructure

## 1.1 Project Setup & Cargo Configuration

### Objective
Create the project structure with proper Cargo.toml configuration, dependencies, and module organization following the design document.

### Actions

1. **Create directory structure**
   ```
   driver/databricks/
   ├── Cargo.toml
   ├── src/
   │   ├── lib.rs
   │   ├── driver.rs
   │   ├── database.rs
   │   ├── connection.rs
   │   ├── statement.rs
   │   ├── error.rs
   │   ├── options.rs
   │   ├── session.rs
   │   ├── client/
   │   │   ├── mod.rs
   │   │   ├── models.rs
   │   │   └── error.rs
   │   └── fetch/
   │       ├── mod.rs
   │       ├── reader.rs
   │       └── decompress.rs
   └── tests/
       ├── integration/
       └── unit/
   ```

2. **Create Cargo.toml with dependencies**
   ```toml
   [package]
   name = "adbc-driver-databricks"
   version = "0.1.0"
   edition = "2021"
   license = "Apache-2.0"
   description = "ADBC driver for Databricks SQL Warehouses"

   [lib]
   crate-type = ["lib", "cdylib"]

   [features]
   default = ["rustls-tls"]
   rustls-tls = ["reqwest/rustls-tls"]
   native-tls = ["reqwest/native-tls"]

   [dependencies]
   adbc_core = { path = "../../core" }
   arrow = { version = "53", default-features = false }
   arrow-ipc = { version = "53" }
   arrow-schema = { version = "53" }
   arrow-array = { version = "53" }
   tokio = { version = "1", features = ["rt-multi-thread", "sync", "time"] }
   reqwest = { version = "0.12", default-features = false, features = ["json", "gzip"] }
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   lz4_flex = "0.11"
   thiserror = "1"
   url = "2"
   base64 = "0.22"
   futures = "0.3"

   [dev-dependencies]
   tokio-test = "0.4"
   wiremock = "0.6"
   ```

3. **Create lib.rs with module declarations**
   ```rust
   //! ADBC driver for Databricks SQL Warehouses

   mod client;
   mod connection;
   mod database;
   mod driver;
   mod error;
   mod fetch;
   mod options;
   mod session;
   mod statement;

   pub use driver::DatabricksDriver;
   pub use database::DatabricksDatabase;
   pub use connection::DatabricksConnection;
   pub use statement::DatabricksStatement;
   pub use error::Error;
   ```

4. **Add to workspace Cargo.toml**
   - Add `"driver/databricks"` to workspace members

### Expected Results

| Result | Verification |
|--------|--------------|
| Project compiles | `cargo build -p adbc-driver-databricks` succeeds |
| Dependencies resolve | No version conflicts in `Cargo.lock` |
| Module structure created | All files exist with basic module declarations |
| Workspace integration | Driver appears in `cargo workspace` output |

### Files Created
- `driver/databricks/Cargo.toml`
- `driver/databricks/src/lib.rs`
- `driver/databricks/src/*.rs` (stub files)
- `driver/databricks/src/client/mod.rs`
- `driver/databricks/src/fetch/mod.rs`

---

## 1.2 Error Types & ADBC Status Mapping

### Objective
Implement comprehensive error handling infrastructure with SEA API error to ADBC status mapping.

### Actions

1. **Define custom error types in `src/error.rs`**
   ```rust
   use adbc_core::error::Status;
   use thiserror::Error;

   #[derive(Error, Debug)]
   pub enum Error {
       #[error("HTTP error: {0}")]
       Http(#[from] reqwest::Error),

       #[error("SEA API error: {code} - {message}")]
       SeaApi { code: String, message: String, http_status: u16 },

       #[error("Arrow error: {0}")]
       Arrow(#[from] arrow::error::ArrowError),

       #[error("JSON error: {0}")]
       Json(#[from] serde_json::Error),

       #[error("Invalid configuration: {0}")]
       Config(String),

       #[error("IO error: {0}")]
       Io(#[from] std::io::Error),

       #[error("Statement failed: {0}")]
       StatementFailed(String),

       #[error("Session error: {0}")]
       Session(String),

       #[error("Timeout waiting for statement")]
       Timeout,
   }
   ```

2. **Implement ADBC Status mapping**
   ```rust
   impl Error {
       pub fn to_adbc_status(&self) -> Status {
           match self {
               Error::SeaApi { code, http_status, .. } => {
                   match (code.as_str(), *http_status) {
                       (_, 400) => Status::InvalidArguments,
                       (_, 401) => Status::Unauthenticated,
                       (_, 403) => Status::Unauthorized,
                       (_, 404) => Status::NotFound,
                       (_, 429) => Status::IO,  // Rate limited, retryable
                       (_, 500) => Status::Internal,
                       (_, 503) => Status::IO,  // Unavailable, retryable
                       _ => Status::Unknown,
                   }
               }
               Error::Http(_) => Status::IO,
               Error::Arrow(_) => Status::InvalidData,
               Error::Json(_) => Status::InvalidData,
               Error::Config(_) => Status::InvalidArguments,
               Error::Io(_) => Status::IO,
               Error::StatementFailed(_) => Status::Internal,
               Error::Session(_) => Status::InvalidState,
               Error::Timeout => Status::Timeout,
           }
       }

       pub fn is_retryable(&self) -> bool {
           match self {
               Error::SeaApi { http_status, .. } => {
                   matches!(*http_status, 429 | 500 | 503)
               }
               Error::Http(e) => e.is_timeout() || e.is_connect(),
               Error::Io(_) => true,
               _ => false,
           }
       }
   }
   ```

3. **Implement conversion to adbc_core::error::Error**
   ```rust
   impl From<Error> for adbc_core::error::Error {
       fn from(err: Error) -> Self {
           adbc_core::error::Error::with_message_and_status(
               err.to_string(),
               err.to_adbc_status(),
           )
       }
   }
   ```

4. **Create Result type alias**
   ```rust
   pub type Result<T> = std::result::Result<T, Error>;
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| All SEA error codes mapped | Unit test covers all codes from design Section 5.2 |
| Error messages preserved | Error Display includes original message |
| Retryable errors identified | `is_retryable()` returns true for 429, 500, 503 |
| ADBC conversion works | `Into<adbc_core::error::Error>` compiles |

### Unit Tests
```rust
#[test]
fn test_sea_error_to_adbc_status() {
    let err = Error::SeaApi {
        code: "BAD_REQUEST".into(),
        message: "Invalid SQL".into(),
        http_status: 400
    };
    assert_eq!(err.to_adbc_status(), Status::InvalidArguments);
}

#[test]
fn test_retryable_errors() {
    let err_429 = Error::SeaApi { code: "".into(), message: "".into(), http_status: 429 };
    let err_400 = Error::SeaApi { code: "".into(), message: "".into(), http_status: 400 };
    assert!(err_429.is_retryable());
    assert!(!err_400.is_retryable());
}
```

### Files Modified/Created
- `driver/databricks/src/error.rs`

---

## 1.3 SEA Client - Core HTTP Infrastructure

### Objective
Implement the foundational HTTP client for SEA API communication with proper authentication and configuration.

### Actions

1. **Define SeaClient struct in `src/client/mod.rs`**
   ```rust
   use reqwest::Client;
   use std::sync::Arc;
   use std::time::Duration;

   pub struct SeaClient {
       http_client: Client,
       host: String,
       token: String,
       warehouse_id: String,
   }

   pub struct SeaClientConfig {
       pub host: String,
       pub token: String,
       pub warehouse_id: String,
       pub connect_timeout: Duration,
       pub read_timeout: Duration,
   }

   impl Default for SeaClientConfig {
       fn default() -> Self {
           Self {
               host: String::new(),
               token: String::new(),
               warehouse_id: String::new(),
               connect_timeout: Duration::from_secs(10),
               read_timeout: Duration::from_secs(300),
           }
       }
   }
   ```

2. **Implement SeaClient constructor**
   ```rust
   impl SeaClient {
       pub fn new(config: SeaClientConfig) -> Result<Self> {
           let http_client = Client::builder()
               .connect_timeout(config.connect_timeout)
               .timeout(config.read_timeout)
               .default_headers(Self::default_headers(&config.token))
               .build()?;

           Ok(Self {
               http_client,
               host: config.host,
               token: config.token,
               warehouse_id: config.warehouse_id,
           })
       }

       fn default_headers(token: &str) -> reqwest::header::HeaderMap {
           let mut headers = reqwest::header::HeaderMap::new();
           headers.insert(
               reqwest::header::AUTHORIZATION,
               format!("Bearer {}", token).parse().unwrap(),
           );
           headers.insert(
               reqwest::header::CONTENT_TYPE,
               "application/json".parse().unwrap(),
           );
           headers.insert(
               reqwest::header::USER_AGENT,
               "adbc-driver-databricks/0.1.0".parse().unwrap(),
           );
           headers
       }
   }
   ```

3. **Implement base URL and endpoint helpers**
   ```rust
   impl SeaClient {
       fn base_url(&self) -> String {
           format!("{}/api/2.0/sql", self.host.trim_end_matches('/'))
       }

       fn statements_url(&self) -> String {
           format!("{}/statements", self.base_url())
       }

       fn statement_url(&self, statement_id: &str) -> String {
           format!("{}/statements/{}", self.base_url(), statement_id)
       }

       fn sessions_url(&self) -> String {
           format!("{}/sessions", self.base_url())
       }

       fn session_url(&self, session_id: &str) -> String {
           format!("{}/sessions/{}", self.base_url(), session_id)
       }

       pub fn warehouse_id(&self) -> &str {
           &self.warehouse_id
       }
   }
   ```

4. **Implement generic request/response handling**
   ```rust
   impl SeaClient {
       async fn post<Req, Resp>(&self, url: &str, body: &Req) -> Result<Resp>
       where
           Req: serde::Serialize,
           Resp: serde::de::DeserializeOwned,
       {
           let response = self.http_client
               .post(url)
               .json(body)
               .send()
               .await?;

           self.handle_response(response).await
       }

       async fn get<Resp>(&self, url: &str) -> Result<Resp>
       where
           Resp: serde::de::DeserializeOwned,
       {
           let response = self.http_client
               .get(url)
               .send()
               .await?;

           self.handle_response(response).await
       }

       async fn delete(&self, url: &str) -> Result<()> {
           let response = self.http_client
               .delete(url)
               .send()
               .await?;

           if response.status().is_success() {
               Ok(())
           } else {
               self.handle_error_response(response).await
           }
       }

       async fn handle_response<Resp>(&self, response: reqwest::Response) -> Result<Resp>
       where
           Resp: serde::de::DeserializeOwned,
       {
           let status = response.status();
           if status.is_success() {
               Ok(response.json().await?)
           } else {
               self.handle_error_response(response).await
           }
       }

       async fn handle_error_response<T>(&self, response: reqwest::Response) -> Result<T> {
           let http_status = response.status().as_u16();
           let body: serde_json::Value = response.json().await.unwrap_or_default();

           Err(Error::SeaApi {
               code: body["error_code"].as_str().unwrap_or("UNKNOWN").to_string(),
               message: body["message"].as_str().unwrap_or("Unknown error").to_string(),
               http_status,
           })
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| SeaClient instantiates | `SeaClient::new(config)` returns Ok |
| Headers set correctly | Request includes Authorization, Content-Type |
| Timeouts configured | Client respects connect and read timeouts |
| Error responses parsed | 4xx/5xx responses return SeaApi error |

### Unit Tests
```rust
#[test]
fn test_base_url_construction() {
    let client = SeaClient::new(SeaClientConfig {
        host: "https://workspace.cloud.databricks.com".into(),
        token: "token".into(),
        warehouse_id: "abc123".into(),
        ..Default::default()
    }).unwrap();

    assert_eq!(client.base_url(), "https://workspace.cloud.databricks.com/api/2.0/sql");
}
```

### Files Modified/Created
- `driver/databricks/src/client/mod.rs`

---

## 1.4 SEA Client - Session Management

### Objective
Implement session creation and deletion endpoints for managing SQL Warehouse sessions.

### Actions

1. **Define session request/response models in `src/client/models.rs`**
   ```rust
   use serde::{Deserialize, Serialize};

   #[derive(Debug, Serialize)]
   pub struct CreateSessionRequest {
       pub warehouse_id: String,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub catalog: Option<String>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub schema: Option<String>,
   }

   #[derive(Debug, Deserialize)]
   pub struct CreateSessionResponse {
       pub session_id: String,
   }
   ```

2. **Implement session endpoints in SeaClient**
   ```rust
   impl SeaClient {
       /// Create a new session with the SQL Warehouse
       pub async fn create_session(
           &self,
           catalog: Option<String>,
           schema: Option<String>,
       ) -> Result<String> {
           let request = CreateSessionRequest {
               warehouse_id: self.warehouse_id.clone(),
               catalog,
               schema,
           };

           let response: CreateSessionResponse = self
               .post(&self.sessions_url(), &request)
               .await?;

           Ok(response.session_id)
       }

       /// Delete/terminate a session
       pub async fn delete_session(&self, session_id: &str) -> Result<()> {
           self.delete(&self.session_url(session_id)).await
       }
   }
   ```

3. **Create session manager in `src/session.rs`**
   ```rust
   use std::sync::Arc;
   use tokio::sync::Mutex;

   pub struct SessionManager {
       client: Arc<SeaClient>,
       session_id: Mutex<Option<String>>,
       catalog: Option<String>,
       schema: Option<String>,
   }

   impl SessionManager {
       pub fn new(
           client: Arc<SeaClient>,
           catalog: Option<String>,
           schema: Option<String>,
       ) -> Self {
           Self {
               client,
               session_id: Mutex::new(None),
               catalog,
               schema,
           }
       }

       /// Get or create a session
       pub async fn get_session_id(&self) -> Result<String> {
           let mut guard = self.session_id.lock().await;

           if let Some(ref id) = *guard {
               return Ok(id.clone());
           }

           let session_id = self.client
               .create_session(self.catalog.clone(), self.schema.clone())
               .await?;

           *guard = Some(session_id.clone());
           Ok(session_id)
       }

       /// Terminate the session if active
       pub async fn terminate(&self) -> Result<()> {
           let mut guard = self.session_id.lock().await;

           if let Some(ref id) = *guard {
               self.client.delete_session(id).await?;
               *guard = None;
           }

           Ok(())
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Session created | API returns session_id |
| Session deleted | DELETE returns 200 |
| Session ID cached | Second call returns same ID |
| Catalog/schema set | Session respects initial catalog/schema |

### Integration Test
```rust
#[tokio::test]
#[ignore] // Requires live SQL Warehouse
async fn test_session_lifecycle() {
    let client = create_test_client();

    // Create session
    let session_id = client.create_session(Some("main".into()), Some("default".into())).await.unwrap();
    assert!(!session_id.is_empty());

    // Delete session
    client.delete_session(&session_id).await.unwrap();
}
```

### Files Modified/Created
- `driver/databricks/src/client/models.rs`
- `driver/databricks/src/client/mod.rs` (add session methods)
- `driver/databricks/src/session.rs`

---

## 1.5 Retry Logic & Exponential Backoff

### Objective
Implement retry strategy for transient errors with exponential backoff and jitter.

### Actions

1. **Define RetryConfig in `src/client/mod.rs`**
   ```rust
   use std::time::Duration;
   use rand::Rng;

   #[derive(Clone, Debug)]
   pub struct RetryConfig {
       pub max_retries: u32,
       pub base_delay: Duration,
       pub max_delay: Duration,
       pub jitter: f64,
   }

   impl Default for RetryConfig {
       fn default() -> Self {
           Self {
               max_retries: 3,
               base_delay: Duration::from_secs(1),
               max_delay: Duration::from_secs(30),
               jitter: 0.5,
           }
       }
   }
   ```

2. **Implement backoff calculation**
   ```rust
   impl RetryConfig {
       /// Calculate delay for a given attempt (0-indexed)
       pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
           let base_ms = self.base_delay.as_millis() as f64;
           let exponential = base_ms * 2_f64.powi(attempt as i32);
           let capped = exponential.min(self.max_delay.as_millis() as f64);

           // Add jitter: delay * (1 + random(0, jitter))
           let mut rng = rand::thread_rng();
           let jitter_factor = 1.0 + rng.gen::<f64>() * self.jitter;
           let final_ms = capped * jitter_factor;

           Duration::from_millis(final_ms as u64)
       }
   }
   ```

3. **Implement retry wrapper**
   ```rust
   impl SeaClient {
       pub async fn with_retry<F, Fut, T>(&self, retry_config: &RetryConfig, mut f: F) -> Result<T>
       where
           F: FnMut() -> Fut,
           Fut: std::future::Future<Output = Result<T>>,
       {
           let mut last_error = None;

           for attempt in 0..=retry_config.max_retries {
               match f().await {
                   Ok(result) => return Ok(result),
                   Err(e) => {
                       if !e.is_retryable() || attempt == retry_config.max_retries {
                           return Err(e);
                       }

                       let delay = retry_config.delay_for_attempt(attempt);
                       tokio::time::sleep(delay).await;
                       last_error = Some(e);
                   }
               }
           }

           Err(last_error.unwrap())
       }
   }
   ```

4. **Handle Retry-After header for 429 responses**
   ```rust
   impl SeaClient {
       async fn handle_error_response_with_retry_after<T>(
           &self,
           response: reqwest::Response
       ) -> Result<(T, Option<Duration>)> {
           let retry_after = response
               .headers()
               .get("retry-after")
               .and_then(|v| v.to_str().ok())
               .and_then(|s| s.parse::<u64>().ok())
               .map(Duration::from_secs);

           let http_status = response.status().as_u16();
           let body: serde_json::Value = response.json().await.unwrap_or_default();

           Err(Error::SeaApiWithRetry {
               code: body["error_code"].as_str().unwrap_or("UNKNOWN").to_string(),
               message: body["message"].as_str().unwrap_or("Unknown error").to_string(),
               http_status,
               retry_after,
           })
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Backoff increases exponentially | Delays: 1s, 2s, 4s, 8s... |
| Jitter applied | Delays vary within expected range |
| Max delay respected | Delay never exceeds 30s |
| 429 uses Retry-After | Respects server-specified delay |
| Non-retryable fails fast | 400 errors don't retry |

### Unit Tests
```rust
#[test]
fn test_exponential_backoff() {
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        jitter: 0.0, // Disable jitter for deterministic test
        ..Default::default()
    };

    assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));
    assert_eq!(config.delay_for_attempt(1), Duration::from_secs(2));
    assert_eq!(config.delay_for_attempt(2), Duration::from_secs(4));
    assert_eq!(config.delay_for_attempt(5), Duration::from_secs(30)); // Capped
}

#[test]
fn test_jitter_range() {
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        jitter: 0.5,
        ..Default::default()
    };

    for _ in 0..100 {
        let delay = config.delay_for_attempt(0);
        assert!(delay >= Duration::from_secs(1));
        assert!(delay <= Duration::from_millis(1500));
    }
}
```

### Files Modified/Created
- `driver/databricks/src/client/mod.rs` (add retry logic)
- `driver/databricks/Cargo.toml` (add `rand` dependency)

---

## 1.6 DatabricksDriver Implementation

### Objective
Implement the ADBC Driver trait as the entry point for creating database connections.

### Actions

1. **Define DatabricksDriver in `src/driver.rs`**
   ```rust
   use adbc_core::{Driver, Optionable};
   use adbc_core::options::{OptionDatabase, OptionValue};

   use crate::database::DatabricksDatabase;
   use crate::error::Result;

   /// ADBC driver for Databricks SQL Warehouses
   pub struct DatabricksDriver;

   impl DatabricksDriver {
       pub fn new() -> Self {
           Self
       }
   }

   impl Default for DatabricksDriver {
       fn default() -> Self {
           Self::new()
       }
   }
   ```

2. **Implement Driver trait**
   ```rust
   impl Driver for DatabricksDriver {
       type DatabaseType = DatabricksDatabase;

       fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
           Ok(DatabricksDatabase::new())
       }

       fn new_database_with_opts(
           &mut self,
           opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
       ) -> adbc_core::error::Result<Self::DatabaseType> {
           let mut db = DatabricksDatabase::new();

           for (key, value) in opts {
               db.set_option(key, value)?;
           }

           Ok(db)
       }
   }
   ```

3. **Add FFI export macro in `src/lib.rs`**
   ```rust
   // Export for FFI (C API compatibility)
   #[cfg(feature = "ffi")]
   adbc_ffi::export_driver!(DatabricksDriverInit, DatabricksDriver);
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Driver instantiates | `DatabricksDriver::new()` succeeds |
| new_database works | Returns DatabricksDatabase |
| new_database_with_opts works | Options applied to database |
| FFI export compiles | cdylib builds without errors |

### Unit Tests
```rust
#[test]
fn test_driver_new() {
    let mut driver = DatabricksDriver::new();
    let db = driver.new_database().unwrap();
    assert!(db.is_some()); // Database created successfully
}

#[test]
fn test_driver_with_opts() {
    let mut driver = DatabricksDriver::new();
    let opts = vec![
        (OptionDatabase::Uri, OptionValue::String("https://test.databricks.com".into())),
    ];
    let db = driver.new_database_with_opts(opts).unwrap();
    // Verify option was set
}
```

### Files Modified/Created
- `driver/databricks/src/driver.rs`
- `driver/databricks/src/lib.rs` (add export)

---

## 1.7 DatabricksDatabase Implementation

### Objective
Implement the Database trait with configuration management and Tokio runtime initialization.

### Actions

1. **Define configuration structs in `src/options.rs`**
   ```rust
   use std::time::Duration;

   /// Database-level configuration
   #[derive(Clone, Debug)]
   pub struct DatabaseConfig {
       pub host: Option<String>,
       pub warehouse_id: Option<String>,
       pub token: Option<String>,
       pub default_catalog: Option<String>,
       pub default_schema: Option<String>,
       pub http_config: HttpConfig,
       pub fetch_config: FetchConfig,
   }

   #[derive(Clone, Debug)]
   pub struct HttpConfig {
       pub connect_timeout: Duration,
       pub read_timeout: Duration,
       pub max_retries: u32,
       pub retry_backoff_base: Duration,
   }

   #[derive(Clone, Debug)]
   pub struct FetchConfig {
       pub concurrency: usize,
       pub compression: String,
   }

   impl Default for DatabaseConfig {
       fn default() -> Self {
           Self {
               host: None,
               warehouse_id: None,
               token: None,
               default_catalog: None,
               default_schema: None,
               http_config: HttpConfig::default(),
               fetch_config: FetchConfig::default(),
           }
       }
   }

   impl Default for HttpConfig {
       fn default() -> Self {
           Self {
               connect_timeout: Duration::from_secs(10),
               read_timeout: Duration::from_secs(300),
               max_retries: 3,
               retry_backoff_base: Duration::from_secs(1),
           }
       }
   }

   impl Default for FetchConfig {
       fn default() -> Self {
           Self {
               concurrency: 8,
               compression: "LZ4_FRAME".to_string(),
           }
       }
   }
   ```

2. **Define custom option keys**
   ```rust
   /// Custom Databricks option keys
   pub mod keys {
       pub const WAREHOUSE_ID: &str = "databricks.warehouse_id";
       pub const TOKEN: &str = "databricks.token";
       pub const CATALOG: &str = "databricks.catalog";
       pub const SCHEMA: &str = "databricks.schema";
       pub const HTTP_CONNECT_TIMEOUT: &str = "databricks.http.connect_timeout";
       pub const HTTP_READ_TIMEOUT: &str = "databricks.http.read_timeout";
       pub const FETCH_CONCURRENCY: &str = "databricks.fetch.concurrency";
       pub const FETCH_COMPRESSION: &str = "databricks.fetch.compression";
   }
   ```

3. **Implement DatabricksDatabase in `src/database.rs`**
   ```rust
   use std::sync::Arc;
   use tokio::runtime::Runtime;
   use adbc_core::{Database, Optionable};
   use adbc_core::options::{OptionDatabase, OptionValue};

   use crate::connection::DatabricksConnection;
   use crate::options::DatabaseConfig;
   use crate::error::Result;

   pub struct DatabricksDatabase {
       config: DatabaseConfig,
       runtime: Option<Arc<Runtime>>,
   }

   impl DatabricksDatabase {
       pub fn new() -> Self {
           Self {
               config: DatabaseConfig::default(),
               runtime: None,
           }
       }

       /// Get or create the Tokio runtime
       fn get_runtime(&mut self) -> Arc<Runtime> {
           if self.runtime.is_none() {
               let rt = Runtime::new().expect("Failed to create Tokio runtime");
               self.runtime = Some(Arc::new(rt));
           }
           self.runtime.as_ref().unwrap().clone()
       }

       /// Validate configuration before creating connection
       fn validate_config(&self) -> Result<()> {
           if self.config.host.is_none() {
               return Err(Error::Config("Missing required option: uri".into()));
           }
           if self.config.warehouse_id.is_none() {
               return Err(Error::Config("Missing required option: databricks.warehouse_id".into()));
           }
           if self.config.token.is_none() {
               return Err(Error::Config("Missing required option: databricks.token".into()));
           }
           Ok(())
       }
   }
   ```

4. **Implement Optionable trait**
   ```rust
   impl Optionable for DatabricksDatabase {
       type Option = OptionDatabase;

       fn set_option(
           &mut self,
           key: Self::Option,
           value: OptionValue,
       ) -> adbc_core::error::Result<()> {
           match key {
               OptionDatabase::Uri => {
                   self.config.host = Some(value.try_into()?);
               }
               OptionDatabase::Other(ref k) => {
                   self.set_custom_option(k, value)?;
               }
               _ => {
                   return Err(adbc_core::error::Error::with_message_and_status(
                       format!("Unknown option: {:?}", key),
                       adbc_core::error::Status::NotImplemented,
                   ));
               }
           }
           Ok(())
       }

       fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
           match key {
               OptionDatabase::Uri => {
                   self.config.host.clone().ok_or_else(|| {
                       adbc_core::error::Error::with_message_and_status(
                           "Option not set: uri",
                           adbc_core::error::Status::NotFound,
                       )
                   })
               }
               _ => Err(adbc_core::error::Error::with_message_and_status(
                   format!("Unknown option: {:?}", key),
                   adbc_core::error::Status::NotFound,
               )),
           }
       }

       // ... implement other get_option_* methods
   }

   impl DatabricksDatabase {
       fn set_custom_option(&mut self, key: &str, value: OptionValue) -> adbc_core::error::Result<()> {
           use crate::options::keys;

           match key {
               keys::WAREHOUSE_ID => {
                   self.config.warehouse_id = Some(value.try_into()?);
               }
               keys::TOKEN => {
                   self.config.token = Some(value.try_into()?);
               }
               keys::CATALOG => {
                   self.config.default_catalog = Some(value.try_into()?);
               }
               keys::SCHEMA => {
                   self.config.default_schema = Some(value.try_into()?);
               }
               keys::FETCH_CONCURRENCY => {
                   let v: i64 = value.try_into()?;
                   self.config.fetch_config.concurrency = v as usize;
               }
               _ => {
                   return Err(adbc_core::error::Error::with_message_and_status(
                       format!("Unknown option: {}", key),
                       adbc_core::error::Status::NotImplemented,
                   ));
               }
           }
           Ok(())
       }
   }
   ```

5. **Implement Database trait**
   ```rust
   impl Database for DatabricksDatabase {
       type ConnectionType = DatabricksConnection;

       fn new_connection(&mut self) -> adbc_core::error::Result<Self::ConnectionType> {
           self.validate_config()?;

           let runtime = self.get_runtime();
           DatabricksConnection::new(self.config.clone(), runtime)
       }

       fn new_connection_with_opts(
           &mut self,
           opts: impl IntoIterator<Item = (adbc_core::options::OptionConnection, OptionValue)>,
       ) -> adbc_core::error::Result<Self::ConnectionType> {
           self.validate_config()?;

           let runtime = self.get_runtime();
           let mut conn = DatabricksConnection::new(self.config.clone(), runtime)?;

           for (key, value) in opts {
               conn.set_option(key, value)?;
           }

           Ok(conn)
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Config validation works | Missing host/token/warehouse fails |
| Options stored correctly | get_option returns set value |
| Runtime created once | Multiple connections share runtime |
| Custom options parsed | databricks.* options work |

### Unit Tests
```rust
#[test]
fn test_database_config_validation() {
    let mut db = DatabricksDatabase::new();
    assert!(db.new_connection().is_err()); // Missing config

    db.set_option(OptionDatabase::Uri, "https://test.databricks.com".into()).unwrap();
    assert!(db.new_connection().is_err()); // Still missing warehouse_id and token
}

#[test]
fn test_database_options() {
    let mut db = DatabricksDatabase::new();
    db.set_option(OptionDatabase::Uri, "https://test.databricks.com".into()).unwrap();

    let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(uri, "https://test.databricks.com");
}
```

### Files Modified/Created
- `driver/databricks/src/options.rs`
- `driver/databricks/src/database.rs`

---

# Sprint 2: Connection & Basic Statement Execution

## 2.1 DatabricksConnection - Core Implementation

### Objective
Implement the Connection trait with session lifecycle management - creating session on connection open and terminating on close/drop.

### Actions

1. **Define DatabricksConnection in `src/connection.rs`**
   ```rust
   use std::sync::Arc;
   use tokio::runtime::Runtime;
   use adbc_core::Connection;

   use crate::client::SeaClient;
   use crate::options::DatabaseConfig;
   use crate::session::SessionManager;
   use crate::statement::DatabricksStatement;
   use crate::error::Result;

   pub struct DatabricksConnection {
       client: Arc<SeaClient>,
       session_manager: Arc<SessionManager>,
       runtime: Arc<Runtime>,
       config: DatabaseConfig,
       current_catalog: Option<String>,
       current_schema: Option<String>,
   }

   impl DatabricksConnection {
       pub fn new(config: DatabaseConfig, runtime: Arc<Runtime>) -> adbc_core::error::Result<Self> {
           // Create SEA client
           let client_config = SeaClientConfig {
               host: config.host.clone().unwrap(),
               token: config.token.clone().unwrap(),
               warehouse_id: config.warehouse_id.clone().unwrap(),
               connect_timeout: config.http_config.connect_timeout,
               read_timeout: config.http_config.read_timeout,
           };

           let client = Arc::new(SeaClient::new(client_config)?);

           // Create session manager
           let session_manager = Arc::new(SessionManager::new(
               client.clone(),
               config.default_catalog.clone(),
               config.default_schema.clone(),
           ));

           // Create session immediately
           let session_id = runtime.block_on(session_manager.get_session_id())?;

           Ok(Self {
               client,
               session_manager,
               runtime,
               config,
               current_catalog: config.default_catalog.clone(),
               current_schema: config.default_schema.clone(),
           })
       }

       /// Get the session ID for this connection
       pub fn session_id(&self) -> Result<String> {
           self.runtime.block_on(self.session_manager.get_session_id())
       }

       /// Get the SEA client
       pub(crate) fn client(&self) -> Arc<SeaClient> {
           self.client.clone()
       }

       /// Get the runtime
       pub(crate) fn runtime(&self) -> Arc<Runtime> {
           self.runtime.clone()
       }
   }
   ```

2. **Implement Drop for session cleanup**
   ```rust
   impl Drop for DatabricksConnection {
       fn drop(&mut self) {
           // Attempt to terminate the session
           // Note: We can't propagate errors from Drop
           let _ = self.runtime.block_on(self.session_manager.terminate());
       }
   }
   ```

3. **Implement Connection trait (partial - basic methods)**
   ```rust
   impl Connection for DatabricksConnection {
       type StatementType = DatabricksStatement;

       fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
           DatabricksStatement::new(
               self.client.clone(),
               self.session_manager.clone(),
               self.runtime.clone(),
               &self.config,
           )
       }

       fn cancel(&mut self) -> adbc_core::error::Result<()> {
           // Cancel any active operations - will be implemented with statement
           Ok(())
       }

       fn commit(&mut self) -> adbc_core::error::Result<()> {
           // Databricks doesn't support transactions
           Err(adbc_core::error::Error::with_message_and_status(
               "Databricks does not support transactions",
               adbc_core::error::Status::NotImplemented,
           ))
       }

       fn rollback(&mut self) -> adbc_core::error::Result<()> {
           // Databricks doesn't support transactions
           Err(adbc_core::error::Error::with_message_and_status(
               "Databricks does not support transactions",
               adbc_core::error::Status::NotImplemented,
           ))
       }

       // Metadata methods will be implemented in Sprint 4
       fn get_info(
           &mut self,
           _codes: Option<&[adbc_core::options::InfoCode]>,
       ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
           todo!("Implemented in Sprint 4")
       }

       fn get_objects(
           &mut self,
           _depth: adbc_core::options::ObjectDepth,
           _catalog: Option<&str>,
           _db_schema: Option<&str>,
           _table_name: Option<&str>,
           _table_types: Option<&[&str]>,
           _column_name: Option<&str>,
       ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
           todo!("Implemented in Sprint 4")
       }

       fn get_table_schema(
           &mut self,
           _catalog: Option<&str>,
           _db_schema: Option<&str>,
           _table_name: &str,
       ) -> adbc_core::error::Result<arrow_schema::Schema> {
           todo!("Implemented in Sprint 4")
       }

       fn get_table_types(
           &mut self,
       ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
           todo!("Implemented in Sprint 4")
       }

       fn read_partition(
           &mut self,
           _partition: &[u8],
       ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
           Err(adbc_core::error::Error::with_message_and_status(
               "Partitioned reads not supported",
               adbc_core::error::Status::NotImplemented,
           ))
       }

       fn get_statistic_names(
           &mut self,
       ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
           Err(adbc_core::error::Error::with_message_and_status(
               "Statistics not supported",
               adbc_core::error::Status::NotImplemented,
           ))
       }

       fn get_statistics(
           &mut self,
           _catalog: Option<&str>,
           _db_schema: Option<&str>,
           _table_name: Option<&str>,
           _approximate: bool,
       ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
           Err(adbc_core::error::Error::with_message_and_status(
               "Statistics not supported",
               adbc_core::error::Status::NotImplemented,
           ))
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Session created on connect | API called, session_id returned |
| Session terminated on drop | DELETE called when connection dropped |
| Client shared | Multiple statements use same client |
| commit/rollback fail | Returns NotImplemented |

### Integration Test
```rust
#[test]
#[ignore]
fn test_connection_lifecycle() {
    let mut driver = DatabricksDriver::new();
    let mut db = create_test_database(&mut driver);

    {
        let conn = db.new_connection().unwrap();
        let session_id = conn.session_id().unwrap();
        assert!(!session_id.is_empty());
    } // Connection dropped here, session terminated
}
```

### Files Modified/Created
- `driver/databricks/src/connection.rs`

---

## 2.2 DatabricksConnection - Optionable Trait

### Objective
Implement option getting/setting for connections including autocommit and catalog/schema settings.

### Actions

1. **Implement Optionable for DatabricksConnection**
   ```rust
   use adbc_core::options::{OptionConnection, OptionValue};

   impl Optionable for DatabricksConnection {
       type Option = OptionConnection;

       fn set_option(
           &mut self,
           key: Self::Option,
           value: OptionValue,
       ) -> adbc_core::error::Result<()> {
           match key {
               OptionConnection::AutoCommit => {
                   let val: String = value.try_into()?;
                   if val != "true" {
                       return Err(adbc_core::error::Error::with_message_and_status(
                           "Databricks only supports autocommit mode",
                           adbc_core::error::Status::InvalidArguments,
                       ));
                   }
                   Ok(())
               }
               OptionConnection::CurrentCatalog => {
                   self.current_catalog = Some(value.try_into()?);
                   Ok(())
               }
               OptionConnection::CurrentDbSchema => {
                   self.current_schema = Some(value.try_into()?);
                   Ok(())
               }
               OptionConnection::Other(ref k) => {
                   Err(adbc_core::error::Error::with_message_and_status(
                       format!("Unknown connection option: {}", k),
                       adbc_core::error::Status::NotImplemented,
                   ))
               }
               _ => {
                   Err(adbc_core::error::Error::with_message_and_status(
                       format!("Unsupported connection option: {:?}", key),
                       adbc_core::error::Status::NotImplemented,
                   ))
               }
           }
       }

       fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
           match key {
               OptionConnection::AutoCommit => Ok("true".to_string()),
               OptionConnection::CurrentCatalog => {
                   self.current_catalog.clone().ok_or_else(|| {
                       adbc_core::error::Error::with_message_and_status(
                           "Current catalog not set",
                           adbc_core::error::Status::NotFound,
                       )
                   })
               }
               OptionConnection::CurrentDbSchema => {
                   self.current_schema.clone().ok_or_else(|| {
                       adbc_core::error::Error::with_message_and_status(
                           "Current schema not set",
                           adbc_core::error::Status::NotFound,
                       )
                   })
               }
               _ => Err(adbc_core::error::Error::with_message_and_status(
                   format!("Unknown option: {:?}", key),
                   adbc_core::error::Status::NotFound,
               )),
           }
       }

       fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
           Err(adbc_core::error::Error::with_message_and_status(
               format!("Option {:?} is not a byte array", key),
               adbc_core::error::Status::NotFound,
           ))
       }

       fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
           Err(adbc_core::error::Error::with_message_and_status(
               format!("Option {:?} is not an integer", key),
               adbc_core::error::Status::NotFound,
           ))
       }

       fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
           Err(adbc_core::error::Error::with_message_and_status(
               format!("Option {:?} is not a double", key),
               adbc_core::error::Status::NotFound,
           ))
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| AutoCommit always true | get returns "true", set to "false" fails |
| Catalog/Schema settable | Can set and retrieve values |
| Unknown options fail | Returns NotImplemented |

### Unit Tests
```rust
#[test]
fn test_connection_autocommit() {
    let mut conn = create_test_connection();

    assert_eq!(
        conn.get_option_string(OptionConnection::AutoCommit).unwrap(),
        "true"
    );

    // Cannot disable autocommit
    assert!(conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("false".into())
    ).is_err());
}
```

### Files Modified/Created
- `driver/databricks/src/connection.rs` (add Optionable impl)

---

## 2.3 SEA Client - Execute Statement

### Objective
Implement the statement execution endpoint with proper request building and response parsing.

### Actions

1. **Define statement request/response models**
   ```rust
   // In src/client/models.rs

   #[derive(Debug, Serialize)]
   pub struct ExecuteStatementRequest {
       pub statement: String,
       pub warehouse_id: String,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub session_id: Option<String>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub catalog: Option<String>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub schema: Option<String>,
       pub wait_timeout: String,
       pub disposition: String,
       pub format: String,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub row_limit: Option<i64>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub byte_limit: Option<i64>,
   }

   impl Default for ExecuteStatementRequest {
       fn default() -> Self {
           Self {
               statement: String::new(),
               warehouse_id: String::new(),
               session_id: None,
               catalog: None,
               schema: None,
               wait_timeout: "10s".to_string(),
               disposition: "INLINE_OR_EXTERNAL_LINKS".to_string(),
               format: "ARROW_STREAM".to_string(),
               row_limit: None,
               byte_limit: None,
           }
       }
   }

   #[derive(Debug, Deserialize)]
   pub struct ExecuteStatementResponse {
       pub statement_id: String,
       pub status: StatementStatus,
       #[serde(default)]
       pub manifest: Option<ResultManifest>,
       #[serde(default)]
       pub result: Option<StatementResult>,
   }

   #[derive(Debug, Deserialize)]
   pub struct StatementStatus {
       pub state: StatementState,
       #[serde(default)]
       pub error: Option<StatementError>,
   }

   #[derive(Debug, Deserialize, PartialEq)]
   #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
   pub enum StatementState {
       Pending,
       Running,
       Succeeded,
       Failed,
       Canceled,
       Closed,
   }

   #[derive(Debug, Deserialize)]
   pub struct StatementError {
       pub error_code: Option<String>,
       pub message: Option<String>,
   }

   #[derive(Debug, Deserialize)]
   pub struct ResultManifest {
       pub format: String,
       pub schema: ManifestSchema,
       pub total_chunk_count: i32,
       pub total_row_count: Option<i64>,
       pub total_byte_count: Option<i64>,
       pub truncated: Option<bool>,
   }

   #[derive(Debug, Deserialize)]
   pub struct ManifestSchema {
       pub columns: Vec<ColumnInfo>,
   }

   #[derive(Debug, Deserialize)]
   pub struct ColumnInfo {
       pub name: String,
       pub type_name: String,
       pub type_text: String,
       pub position: i32,
       #[serde(default)]
       pub nullable: bool,
   }

   #[derive(Debug, Deserialize)]
   pub struct StatementResult {
       #[serde(default)]
       pub data_array: Option<Vec<Vec<serde_json::Value>>>,
       #[serde(default)]
       pub chunk_index: Option<i32>,
       #[serde(default)]
       pub row_offset: Option<i64>,
       #[serde(default)]
       pub row_count: Option<i64>,
       #[serde(default)]
       pub external_links: Option<Vec<ExternalLink>>,
   }

   #[derive(Debug, Deserialize, Clone)]
   pub struct ExternalLink {
       pub external_link: String,
       pub chunk_index: i32,
       pub row_offset: i64,
       pub row_count: i64,
       pub byte_count: i64,
       pub expiration: String,
   }
   ```

2. **Implement execute_statement in SeaClient**
   ```rust
   impl SeaClient {
       /// Execute a SQL statement
       pub async fn execute_statement(
           &self,
           request: ExecuteStatementRequest,
       ) -> Result<ExecuteStatementResponse> {
           self.post(&self.statements_url(), &request).await
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Request serializes correctly | JSON matches SEA API spec |
| Response deserializes | All fields parsed correctly |
| Statement ID returned | Non-empty string |
| Inline results parsed | data_array or external_links present |

### Unit Tests
```rust
#[test]
fn test_execute_request_serialization() {
    let request = ExecuteStatementRequest {
        statement: "SELECT 1".to_string(),
        warehouse_id: "abc123".to_string(),
        session_id: Some("session456".to_string()),
        ..Default::default()
    };

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("ARROW_STREAM"));
    assert!(json.contains("INLINE_OR_EXTERNAL_LINKS"));
}

#[test]
fn test_execute_response_deserialization() {
    let json = r#"{
        "statement_id": "stmt123",
        "status": { "state": "SUCCEEDED" },
        "result": { "chunk_index": 0, "row_count": 1 }
    }"#;

    let response: ExecuteStatementResponse = serde_json::from_str(json).unwrap();
    assert_eq!(response.statement_id, "stmt123");
    assert_eq!(response.status.state, StatementState::Succeeded);
}
```

### Files Modified/Created
- `driver/databricks/src/client/models.rs`
- `driver/databricks/src/client/mod.rs` (add execute_statement)

---

## 2.4 SEA Client - Statement Polling

### Objective
Implement statement status polling with exponential backoff for async statement execution.

### Actions

1. **Implement get_statement in SeaClient**
   ```rust
   impl SeaClient {
       /// Get statement status and results
       pub async fn get_statement(&self, statement_id: &str) -> Result<ExecuteStatementResponse> {
           self.get(&self.statement_url(statement_id)).await
       }
   }
   ```

2. **Implement polling logic**
   ```rust
   /// Configuration for polling
   #[derive(Clone, Debug)]
   pub struct PollConfig {
       pub initial_delay: Duration,
       pub max_delay: Duration,
       pub timeout: Duration,
   }

   impl Default for PollConfig {
       fn default() -> Self {
           Self {
               initial_delay: Duration::from_secs(1),
               max_delay: Duration::from_secs(10),
               timeout: Duration::from_secs(300),
           }
       }
   }

   impl SeaClient {
       /// Poll until statement completes or fails
       pub async fn poll_until_complete(
           &self,
           statement_id: &str,
           config: &PollConfig,
       ) -> Result<ExecuteStatementResponse> {
           let start = std::time::Instant::now();
           let mut delay = config.initial_delay;

           loop {
               if start.elapsed() > config.timeout {
                   return Err(Error::Timeout);
               }

               let response = self.get_statement(statement_id).await?;

               match response.status.state {
                   StatementState::Succeeded => return Ok(response),
                   StatementState::Failed => {
                       let error_msg = response.status.error
                           .map(|e| e.message.unwrap_or_default())
                           .unwrap_or_else(|| "Unknown error".to_string());
                       return Err(Error::StatementFailed(error_msg));
                   }
                   StatementState::Canceled => {
                       return Err(Error::StatementFailed("Statement was canceled".into()));
                   }
                   StatementState::Pending | StatementState::Running => {
                       tokio::time::sleep(delay).await;
                       // Exponential backoff with cap
                       delay = std::cmp::min(delay * 2, config.max_delay);
                   }
                   StatementState::Closed => {
                       return Err(Error::StatementFailed("Statement was closed".into()));
                   }
               }
           }
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Polling respects delay | First delay ~1s, doubles each iteration |
| Max delay capped | Never exceeds 10s between polls |
| Timeout works | Returns Timeout error after 300s |
| Failed state handled | Returns error with message |

### Unit Tests
```rust
#[tokio::test]
async fn test_poll_backoff_timing() {
    // Use mock server to simulate PENDING -> RUNNING -> SUCCEEDED
    let mock_server = MockServer::start().await;

    // First call: PENDING
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt1",
            "status": { "state": "PENDING" }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second call: SUCCEEDED
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt1",
            "status": { "state": "SUCCEEDED" },
            "result": {}
        })))
        .mount(&mock_server)
        .await;

    let client = create_client_with_url(&mock_server.uri());
    let result = client.poll_until_complete("stmt1", &PollConfig::default()).await;

    assert!(result.is_ok());
}
```

### Files Modified/Created
- `driver/databricks/src/client/mod.rs` (add get_statement, poll_until_complete)

---

## 2.5 DatabricksStatement - Core Implementation

### Objective
Implement the Statement struct with SQL query setting and connection reference management.

### Actions

1. **Define DatabricksStatement in `src/statement.rs`**
   ```rust
   use std::sync::Arc;
   use tokio::runtime::Runtime;
   use adbc_core::{Statement, Optionable};
   use adbc_core::options::{OptionStatement, OptionValue};

   use crate::client::SeaClient;
   use crate::session::SessionManager;
   use crate::options::DatabaseConfig;
   use crate::error::Result;

   pub struct DatabricksStatement {
       client: Arc<SeaClient>,
       session_manager: Arc<SessionManager>,
       runtime: Arc<Runtime>,
       config: StatementConfig,
       sql_query: Option<String>,
       statement_id: Option<String>,
   }

   #[derive(Clone, Debug)]
   pub struct StatementConfig {
       pub wait_timeout: String,
       pub row_limit: Option<i64>,
       pub byte_limit: Option<i64>,
       pub catalog: Option<String>,
       pub schema: Option<String>,
       pub fetch_concurrency: usize,
   }

   impl DatabricksStatement {
       pub fn new(
           client: Arc<SeaClient>,
           session_manager: Arc<SessionManager>,
           runtime: Arc<Runtime>,
           db_config: &DatabaseConfig,
       ) -> adbc_core::error::Result<Self> {
           Ok(Self {
               client,
               session_manager,
               runtime,
               config: StatementConfig {
                   wait_timeout: "10s".to_string(),
                   row_limit: None,
                   byte_limit: None,
                   catalog: db_config.default_catalog.clone(),
                   schema: db_config.default_schema.clone(),
                   fetch_concurrency: db_config.fetch_config.concurrency,
               },
               sql_query: None,
               statement_id: None,
           })
       }
   }
   ```

2. **Implement Optionable for DatabricksStatement**
   ```rust
   impl Optionable for DatabricksStatement {
       type Option = OptionStatement;

       fn set_option(
           &mut self,
           key: Self::Option,
           value: OptionValue,
       ) -> adbc_core::error::Result<()> {
           match key {
               OptionStatement::Other(ref k) => {
                   match k.as_str() {
                       "databricks.statement.wait_timeout" => {
                           self.config.wait_timeout = value.try_into()?;
                       }
                       "databricks.statement.row_limit" => {
                           self.config.row_limit = Some(value.try_into()?);
                       }
                       "databricks.statement.byte_limit" => {
                           self.config.byte_limit = Some(value.try_into()?);
                       }
                       _ => {
                           return Err(adbc_core::error::Error::with_message_and_status(
                               format!("Unknown option: {}", k),
                               adbc_core::error::Status::NotImplemented,
                           ));
                       }
                   }
               }
               _ => {
                   return Err(adbc_core::error::Error::with_message_and_status(
                       format!("Unsupported option: {:?}", key),
                       adbc_core::error::Status::NotImplemented,
                   ));
               }
           }
           Ok(())
       }

       fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
           match key {
               OptionStatement::Other(ref k) if k == "databricks.statement.wait_timeout" => {
                   Ok(self.config.wait_timeout.clone())
               }
               _ => Err(adbc_core::error::Error::with_message_and_status(
                   format!("Unknown option: {:?}", key),
                   adbc_core::error::Status::NotFound,
               )),
           }
       }

       fn get_option_bytes(&self, _key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
           Err(adbc_core::error::Error::with_message_and_status(
               "No byte options",
               adbc_core::error::Status::NotFound,
           ))
       }

       fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
           match key {
               OptionStatement::Other(ref k) if k == "databricks.statement.row_limit" => {
                   self.config.row_limit.ok_or_else(|| {
                       adbc_core::error::Error::with_message_and_status(
                           "row_limit not set",
                           adbc_core::error::Status::NotFound,
                       )
                   })
               }
               _ => Err(adbc_core::error::Error::with_message_and_status(
                   format!("Unknown option: {:?}", key),
                   adbc_core::error::Status::NotFound,
               )),
           }
       }

       fn get_option_double(&self, _key: Self::Option) -> adbc_core::error::Result<f64> {
           Err(adbc_core::error::Error::with_message_and_status(
               "No double options",
               adbc_core::error::Status::NotFound,
           ))
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Statement created | new() returns Ok |
| Options configurable | wait_timeout, row_limit work |
| SQL query stored | set_sql_query stores query |
| Config from database | Inherits catalog/schema |

### Files Modified/Created
- `driver/databricks/src/statement.rs`

---

## 2.6 Async/Sync Bridge

### Objective
Implement the bridge pattern between sync ADBC trait methods and async internal implementation.

### Actions

1. **Add async internal methods to DatabricksStatement**
   ```rust
   impl DatabricksStatement {
       /// Internal async execute implementation
       async fn execute_async(&mut self) -> Result<ArrowResultReader> {
           let sql = self.sql_query.as_ref()
               .ok_or_else(|| Error::Config("No SQL query set".into()))?;

           let session_id = self.session_manager.get_session_id().await?;

           let request = ExecuteStatementRequest {
               statement: sql.clone(),
               warehouse_id: self.client.warehouse_id().to_string(),
               session_id: Some(session_id),
               catalog: self.config.catalog.clone(),
               schema: self.config.schema.clone(),
               wait_timeout: self.config.wait_timeout.clone(),
               row_limit: self.config.row_limit,
               byte_limit: self.config.byte_limit,
               ..Default::default()
           };

           let response = self.client.execute_statement(request).await?;
           self.statement_id = Some(response.statement_id.clone());

           self.handle_execute_response(response).await
       }

       async fn handle_execute_response(
           &self,
           response: ExecuteStatementResponse,
       ) -> Result<ArrowResultReader> {
           match response.status.state {
               StatementState::Succeeded => {
                   self.create_reader_from_result(response).await
               }
               StatementState::Pending | StatementState::Running => {
                   // Poll until complete
                   let final_response = self.client
                       .poll_until_complete(&response.statement_id, &PollConfig::default())
                       .await?;
                   self.create_reader_from_result(final_response).await
               }
               StatementState::Failed => {
                   let msg = response.status.error
                       .and_then(|e| e.message)
                       .unwrap_or_else(|| "Unknown error".into());
                   Err(Error::StatementFailed(msg))
               }
               _ => Err(Error::StatementFailed("Unexpected statement state".into())),
           }
       }
   }
   ```

2. **Implement sync wrapper methods**
   ```rust
   impl Statement for DatabricksStatement {
       fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
           self.sql_query = Some(query.as_ref().to_string());
           Ok(())
       }

       fn execute(&mut self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
           self.runtime.clone().block_on(self.execute_async())
               .map_err(Into::into)
       }

       fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
           let reader = self.runtime.clone().block_on(self.execute_async())
               .map_err(Into::into)?;

           // Consume the reader to get affected rows
           // For DDL, this is typically None
           // For DML, extract from response
           Ok(reader.affected_rows())
       }

       fn cancel(&mut self) -> adbc_core::error::Result<()> {
           if let Some(ref stmt_id) = self.statement_id {
               self.runtime.clone().block_on(self.client.cancel_statement(stmt_id))
                   .map_err(Into::into)?;
           }
           Ok(())
       }

       // ... other Statement trait methods
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Sync methods work | execute() returns without blocking forever |
| Async logic preserved | Polling happens correctly |
| Runtime shared | No new runtime per call |
| No deadlocks | Concurrent operations work |

### Files Modified/Created
- `driver/databricks/src/statement.rs` (add async/sync bridge)

---

## 2.7 Arrow Result Reader - Inline Results

### Objective
Implement Arrow IPC parsing for inline results returned directly in the API response.

### Actions

1. **Define ArrowResultReader in `src/fetch/reader.rs`**
   ```rust
   use arrow_array::RecordBatch;
   use arrow_schema::SchemaRef;
   use std::sync::Arc;

   pub struct ArrowResultReader {
       schema: SchemaRef,
       batches: Vec<RecordBatch>,
       current_index: usize,
       affected_rows: Option<i64>,
   }

   impl ArrowResultReader {
       /// Create reader from inline Arrow data
       pub fn from_inline_data(
           schema: SchemaRef,
           data: &[u8],
       ) -> Result<Self> {
           let cursor = std::io::Cursor::new(data);
           let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)?;

           let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;

           Ok(Self {
               schema,
               batches,
               current_index: 0,
               affected_rows: None,
           })
       }

       /// Create empty reader (for DDL statements)
       pub fn empty(schema: SchemaRef) -> Self {
           Self {
               schema,
               batches: Vec::new(),
               current_index: 0,
               affected_rows: None,
           }
       }

       /// Create reader with affected rows count
       pub fn with_affected_rows(mut self, rows: i64) -> Self {
           self.affected_rows = Some(rows);
           self
       }

       pub fn affected_rows(&self) -> Option<i64> {
           self.affected_rows
       }
   }
   ```

2. **Implement Iterator and RecordBatchReader traits**
   ```rust
   impl Iterator for ArrowResultReader {
       type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

       fn next(&mut self) -> Option<Self::Item> {
           if self.current_index < self.batches.len() {
               let batch = self.batches[self.current_index].clone();
               self.current_index += 1;
               Some(Ok(batch))
           } else {
               None
           }
       }
   }

   impl arrow_array::RecordBatchReader for ArrowResultReader {
       fn schema(&self) -> SchemaRef {
           self.schema.clone()
       }
   }
   ```

3. **Implement schema conversion from SEA manifest**
   ```rust
   use arrow_schema::{DataType, Field, Schema};
   use crate::client::models::{ManifestSchema, ColumnInfo};

   pub fn manifest_to_arrow_schema(manifest: &ManifestSchema) -> Result<SchemaRef> {
       let fields: Vec<Field> = manifest.columns
           .iter()
           .map(|col| {
               let data_type = spark_type_to_arrow(&col.type_name)?;
               Ok(Field::new(&col.name, data_type, col.nullable))
           })
           .collect::<Result<Vec<_>>>()?;

       Ok(Arc::new(Schema::new(fields)))
   }

   fn spark_type_to_arrow(spark_type: &str) -> Result<DataType> {
       match spark_type.to_uppercase().as_str() {
           "BOOLEAN" => Ok(DataType::Boolean),
           "TINYINT" | "BYTE" => Ok(DataType::Int8),
           "SMALLINT" | "SHORT" => Ok(DataType::Int16),
           "INT" | "INTEGER" => Ok(DataType::Int32),
           "BIGINT" | "LONG" => Ok(DataType::Int64),
           "FLOAT" | "REAL" => Ok(DataType::Float32),
           "DOUBLE" => Ok(DataType::Float64),
           "STRING" => Ok(DataType::Utf8),
           "BINARY" => Ok(DataType::Binary),
           "DATE" => Ok(DataType::Date32),
           "TIMESTAMP" => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),
           s if s.starts_with("DECIMAL") => {
               // Parse DECIMAL(p,s)
               parse_decimal_type(s)
           }
           s if s.starts_with("ARRAY") => {
               // Parse ARRAY<T>
               parse_array_type(s)
           }
           s if s.starts_with("MAP") => {
               // Parse MAP<K,V>
               parse_map_type(s)
           }
           s if s.starts_with("STRUCT") => {
               // Parse STRUCT<...>
               parse_struct_type(s)
           }
           _ => Err(Error::Config(format!("Unknown Spark type: {}", spark_type))),
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| IPC data parsed | Arrow RecordBatches created |
| Schema correct | Matches Spark SQL types |
| Iterator works | Can iterate through batches |
| Empty results handled | DDL returns empty reader |

### Unit Tests
```rust
#[test]
fn test_spark_type_mapping() {
    assert_eq!(spark_type_to_arrow("INT").unwrap(), DataType::Int32);
    assert_eq!(spark_type_to_arrow("STRING").unwrap(), DataType::Utf8);
    assert_eq!(spark_type_to_arrow("BOOLEAN").unwrap(), DataType::Boolean);
}

#[test]
fn test_reader_iteration() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    // Create test batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let reader = ArrowResultReader {
        schema,
        batches: vec![batch],
        current_index: 0,
        affected_rows: None,
    };

    let results: Vec<_> = reader.collect();
    assert_eq!(results.len(), 1);
}
```

### Files Modified/Created
- `driver/databricks/src/fetch/reader.rs`
- `driver/databricks/src/fetch/mod.rs`

---

## 2.8 Statement Execute - Inline Path

### Objective
Complete the execute() method for queries returning inline results (small result sets).

### Actions

1. **Implement create_reader_from_result in DatabricksStatement**
   ```rust
   impl DatabricksStatement {
       async fn create_reader_from_result(
           &self,
           response: ExecuteStatementResponse,
       ) -> Result<ArrowResultReader> {
           let manifest = response.manifest
               .ok_or_else(|| Error::StatementFailed("No manifest in response".into()))?;

           let schema = manifest_to_arrow_schema(&manifest.schema)?;

           let result = response.result
               .ok_or_else(|| Error::StatementFailed("No result in response".into()))?;

           // Check if this is an inline result or external links
           if let Some(ref external_links) = result.external_links {
               // External links - will be handled in Sprint 3
               todo!("External links handling in Sprint 3")
           } else if manifest.total_row_count == Some(0) {
               // Empty result
               Ok(ArrowResultReader::empty(schema))
           } else {
               // Inline result - data should be in the response
               // For ARROW_STREAM format, the inline data is base64 encoded
               let data = self.extract_inline_arrow_data(&response)?;
               ArrowResultReader::from_inline_data(schema, &data)
           }
       }

       fn extract_inline_arrow_data(&self, response: &ExecuteStatementResponse) -> Result<Vec<u8>> {
           // The inline Arrow data may be in different locations depending on response
           // This extracts and decodes it

           // For ARROW_STREAM with inline disposition, data is typically base64 encoded
           if let Some(ref result) = response.result {
               // Check for inline binary data field
               // Note: Actual field name may vary - check SEA API docs
               // This is a placeholder implementation
               Ok(Vec::new()) // Will be populated with actual parsing
           } else {
               Err(Error::StatementFailed("No inline data found".into()))
           }
       }
   }
   ```

2. **Add integration test for end-to-end inline query**
   ```rust
   #[test]
   #[ignore] // Requires live Databricks
   fn test_simple_select_query() {
       let mut driver = DatabricksDriver::new();
       let mut db = create_test_database(&mut driver);
       let mut conn = db.new_connection().unwrap();
       let mut stmt = conn.new_statement().unwrap();

       stmt.set_sql_query("SELECT 1 as num, 'hello' as msg").unwrap();
       let reader = stmt.execute().unwrap();

       let schema = reader.schema();
       assert_eq!(schema.fields().len(), 2);

       let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
       assert!(!batches.is_empty());
       assert_eq!(batches[0].num_rows(), 1);
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Simple SELECT works | Returns expected data |
| Schema correct | Column names and types match |
| Data parseable | RecordBatch iteration succeeds |
| Empty results handled | Zero-row queries work |

### Files Modified/Created
- `driver/databricks/src/statement.rs` (add create_reader_from_result)
- `driver/databricks/tests/integration/basic_query.rs`

---

# Sprint 3: External Links & Parallel Chunk Fetching

## 3.1 SEA Client - Get Chunk Endpoint

### Objective
Implement the get_chunk endpoint for retrieving refreshed external links when URLs expire.

### Actions

1. **Define chunk response model**
   ```rust
   // In src/client/models.rs

   #[derive(Debug, Deserialize)]
   pub struct GetChunkResponse {
       pub external_links: Vec<ExternalLink>,
   }
   ```

2. **Implement get_chunk in SeaClient**
   ```rust
   impl SeaClient {
       /// Get chunk with refreshed external links
       pub async fn get_chunk(
           &self,
           statement_id: &str,
           chunk_index: i32,
       ) -> Result<GetChunkResponse> {
           let url = format!(
               "{}/result/chunks/{}",
               self.statement_url(statement_id),
               chunk_index
           );
           self.get(&url).await
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Chunk URL constructed correctly | Matches SEA API spec |
| Fresh links returned | New expiration time |
| Error handling works | 404 for invalid chunk |

---

## 3.2 LZ4 Decompression

### Objective
Implement LZ4_FRAME decompression for compressed Arrow IPC data from cloud storage.

### Actions

1. **Create decompression module `src/fetch/decompress.rs`**
   ```rust
   use lz4_flex::frame::FrameDecoder;
   use std::io::Read;

   /// Decompress LZ4_FRAME compressed data
   pub fn decompress_lz4(compressed: &[u8]) -> Result<Vec<u8>> {
       let mut decoder = FrameDecoder::new(compressed);
       let mut decompressed = Vec::new();
       decoder.read_to_end(&mut decompressed)?;
       Ok(decompressed)
   }

   /// Check if data appears to be LZ4 compressed
   pub fn is_lz4_compressed(data: &[u8]) -> bool {
       // LZ4 frame magic number: 0x184D2204
       data.len() >= 4 && data[0..4] == [0x04, 0x22, 0x4D, 0x18]
   }

   /// Decompress if needed, return original if not compressed
   pub fn decompress_if_needed(data: Vec<u8>) -> Result<Vec<u8>> {
       if is_lz4_compressed(&data) {
           decompress_lz4(&data)
       } else {
           Ok(data)
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| LZ4 decompression works | Compressed data -> original |
| Non-compressed passthrough | Uncompressed data unchanged |
| Magic number detection | Correctly identifies LZ4 |
| Error on corrupt data | Returns error, doesn't panic |

### Unit Tests
```rust
#[test]
fn test_lz4_decompression() {
    let original = b"Hello, World! This is test data for compression.";

    // Compress
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder.write_all(original).unwrap();
    let compressed = encoder.finish().unwrap();

    // Decompress
    let decompressed = decompress_lz4(&compressed).unwrap();
    assert_eq!(decompressed, original);
}

#[test]
fn test_is_lz4_compressed() {
    let lz4_header = [0x04, 0x22, 0x4D, 0x18, 0x00, 0x00];
    let not_lz4 = [0x00, 0x00, 0x00, 0x00];

    assert!(is_lz4_compressed(&lz4_header));
    assert!(!is_lz4_compressed(&not_lz4));
}
```

---

## 3.3 ChunkFetcher - Core Implementation

### Objective
Implement parallel chunk fetching infrastructure for downloading Arrow data from cloud storage.

### Actions

1. **Define ChunkFetcher in `src/fetch/mod.rs`**
   ```rust
   use std::sync::Arc;
   use reqwest::Client;
   use tokio::sync::Semaphore;

   use crate::client::models::ExternalLink;

   pub struct ChunkFetcher {
       http_client: Client,
       sea_client: Arc<SeaClient>,
       statement_id: String,
       concurrency: usize,
   }

   impl ChunkFetcher {
       pub fn new(
           sea_client: Arc<SeaClient>,
           statement_id: String,
           concurrency: usize,
       ) -> Result<Self> {
           let http_client = Client::builder()
               .timeout(Duration::from_secs(300))
               .build()?;

           Ok(Self {
               http_client,
               sea_client,
               statement_id,
               concurrency,
           })
       }

       /// Fetch a single chunk from its external link
       async fn fetch_chunk(&self, link: &ExternalLink) -> Result<Vec<u8>> {
           let response = self.http_client
               .get(&link.external_link)
               .send()
               .await?;

           if response.status() == reqwest::StatusCode::FORBIDDEN {
               // URL might be expired, need to refresh
               return Err(Error::UrlExpired(link.chunk_index));
           }

           if !response.status().is_success() {
               return Err(Error::Http(
                   format!("Failed to fetch chunk {}: {}", link.chunk_index, response.status())
               ));
           }

           let bytes = response.bytes().await?;
           Ok(bytes.to_vec())
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| HTTP client configured | Timeout set correctly |
| Single chunk fetchable | Presigned URL works |
| 403 detected | Returns UrlExpired error |

---

## 3.4 ChunkFetcher - Ordered Output

### Objective
Ensure chunks are yielded in correct order (0, 1, 2, ...) despite being fetched in parallel.

### Actions

1. **Implement ordered parallel fetching**
   ```rust
   use tokio::sync::mpsc;
   use futures::stream::{self, StreamExt};

   impl ChunkFetcher {
       /// Fetch all chunks in parallel, yield in order
       pub async fn fetch_all_chunks(
           &self,
           links: Vec<ExternalLink>,
       ) -> Result<Vec<Vec<u8>>> {
           let semaphore = Arc::new(Semaphore::new(self.concurrency));
           let total_chunks = links.len();

           // Spawn all fetch tasks
           let tasks: Vec<_> = links.into_iter()
               .map(|link| {
                   let sem = semaphore.clone();
                   let fetcher = self.clone();
                   let chunk_index = link.chunk_index;

                   tokio::spawn(async move {
                       let _permit = sem.acquire().await.unwrap();
                       let data = fetcher.fetch_chunk_with_retry(&link).await?;
                       Ok::<_, Error>((chunk_index, data))
                   })
               })
               .collect();

           // Collect results
           let mut results: Vec<Option<Vec<u8>>> = vec![None; total_chunks];

           for task in tasks {
               let (index, data) = task.await??;
               results[index as usize] = Some(data);
           }

           // Convert to ordered vec
           results.into_iter()
               .map(|opt| opt.ok_or_else(|| Error::StatementFailed("Missing chunk".into())))
               .collect()
       }
   }
   ```

2. **Implement streaming variant**
   ```rust
   impl ChunkFetcher {
       /// Stream chunks in order as they become available
       pub fn fetch_chunks_stream(
           self: Arc<Self>,
           links: Vec<ExternalLink>,
       ) -> impl Stream<Item = Result<RecordBatch>> {
           let (tx, rx) = mpsc::channel(self.concurrency * 2);
           let total = links.len();

           // Spawn fetcher task
           tokio::spawn(async move {
               let mut results: Vec<Option<Vec<u8>>> = vec![None; total];
               let mut next_to_send = 0;

               // ... fetch and buffer logic
               // Send in order as chunks become available
           });

           tokio_stream::wrappers::ReceiverStream::new(rx)
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Chunks in order | Index 0, 1, 2... regardless of fetch order |
| Parallelism works | Multiple downloads concurrent |
| Semaphore limits | Never exceeds concurrency |
| Missing chunk fails | Error if any chunk missing |

---

## 3.5 ChunkFetcher - URL Expiration Handling

### Objective
Implement automatic URL refresh when presigned URLs expire during download.

### Actions

1. **Implement fetch with retry and refresh**
   ```rust
   impl ChunkFetcher {
       async fn fetch_chunk_with_retry(&self, link: &ExternalLink) -> Result<Vec<u8>> {
           const MAX_RETRIES: u32 = 3;

           for attempt in 0..MAX_RETRIES {
               match self.fetch_chunk(link).await {
                   Ok(data) => return Ok(data),
                   Err(Error::UrlExpired(chunk_index)) => {
                       // Refresh the URL
                       let refreshed = self.refresh_chunk_link(chunk_index).await?;

                       // Try again with refreshed link
                       match self.fetch_chunk(&refreshed).await {
                           Ok(data) => return Ok(data),
                           Err(e) if attempt < MAX_RETRIES - 1 => {
                               tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                               continue;
                           }
                           Err(e) => return Err(e),
                       }
                   }
                   Err(e) if e.is_retryable() && attempt < MAX_RETRIES - 1 => {
                       tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                       continue;
                   }
                   Err(e) => return Err(e),
               }
           }

           Err(Error::StatementFailed("Max retries exceeded".into()))
       }

       async fn refresh_chunk_link(&self, chunk_index: i32) -> Result<ExternalLink> {
           let response = self.sea_client
               .get_chunk(&self.statement_id, chunk_index)
               .await?;

           response.external_links
               .into_iter()
               .find(|l| l.chunk_index == chunk_index)
               .ok_or_else(|| Error::StatementFailed("Chunk not found in refresh".into()))
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Expired URL refreshed | get_chunk called on 403 |
| Retry with new URL | Download succeeds after refresh |
| Max retries respected | Fails after 3 attempts |

---

## 3.6 Arrow Result Reader - External Links

### Objective
Extend the ArrowResultReader to support streaming results from ChunkFetcher.

### Actions

1. **Create streaming reader variant**
   ```rust
   pub struct StreamingArrowReader {
       schema: SchemaRef,
       chunk_receiver: mpsc::Receiver<Result<RecordBatch>>,
       current_batch: Option<RecordBatch>,
   }

   impl StreamingArrowReader {
       pub fn new(
           schema: SchemaRef,
           chunk_receiver: mpsc::Receiver<Result<RecordBatch>>,
       ) -> Self {
           Self {
               schema,
               chunk_receiver,
               current_batch: None,
           }
       }
   }

   impl Iterator for StreamingArrowReader {
       type Item = std::result::Result<RecordBatch, ArrowError>;

       fn next(&mut self) -> Option<Self::Item> {
           match self.chunk_receiver.blocking_recv() {
               Some(Ok(batch)) => Some(Ok(batch)),
               Some(Err(e)) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
               None => None,
           }
       }
   }

   impl RecordBatchReader for StreamingArrowReader {
       fn schema(&self) -> SchemaRef {
           self.schema.clone()
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Streaming works | Batches yielded as available |
| Memory bounded | Only buffered batches in memory |
| Errors propagated | Fetch errors surface to caller |

---

## 3.7 Statement Execute - External Links Path

### Objective
Complete execute() implementation for queries returning external links (large result sets).

### Actions

1. **Update create_reader_from_result to handle external links**
   ```rust
   impl DatabricksStatement {
       async fn create_reader_from_result(
           &self,
           response: ExecuteStatementResponse,
       ) -> Result<Box<dyn RecordBatchReader + Send>> {
           let manifest = response.manifest
               .ok_or_else(|| Error::StatementFailed("No manifest".into()))?;

           let schema = manifest_to_arrow_schema(&manifest.schema)?;

           if let Some(ref result) = response.result {
               if let Some(ref external_links) = result.external_links {
                   // Large result with external links
                   return self.create_streaming_reader(
                       schema,
                       external_links.clone(),
                       response.statement_id,
                   ).await;
               }
           }

           // Inline or empty result
           if manifest.total_row_count == Some(0) {
               Ok(Box::new(ArrowResultReader::empty(schema)))
           } else {
               let data = self.extract_inline_arrow_data(&response)?;
               Ok(Box::new(ArrowResultReader::from_inline_data(schema, &data)?))
           }
       }

       async fn create_streaming_reader(
           &self,
           schema: SchemaRef,
           external_links: Vec<ExternalLink>,
           statement_id: String,
       ) -> Result<Box<dyn RecordBatchReader + Send>> {
           let fetcher = Arc::new(ChunkFetcher::new(
               self.client.clone(),
               statement_id,
               self.config.fetch_concurrency,
           )?);

           let (tx, rx) = mpsc::channel(self.config.fetch_concurrency * 2);

           // Spawn background fetching
           let schema_clone = schema.clone();
           tokio::spawn(async move {
               match fetcher.fetch_all_chunks(external_links).await {
                   Ok(chunks) => {
                       for chunk_data in chunks {
                           let decompressed = decompress_if_needed(chunk_data)?;
                           let batch = parse_arrow_ipc(&decompressed, &schema_clone)?;
                           if tx.send(Ok(batch)).await.is_err() {
                               break; // Receiver dropped
                           }
                       }
                   }
                   Err(e) => {
                       let _ = tx.send(Err(e)).await;
                   }
               }
           });

           Ok(Box::new(StreamingArrowReader::new(schema, rx)))
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Large queries work | >1GB results stream correctly |
| Chunks decompressed | LZ4 data handled |
| Memory efficient | Constant memory usage |
| All rows returned | Row count matches manifest |

---

## 3.8 Statement Cancel

### Objective
Implement statement cancellation for stopping in-progress queries.

### Actions

1. **Implement cancel_statement in SeaClient**
   ```rust
   impl SeaClient {
       pub async fn cancel_statement(&self, statement_id: &str) -> Result<()> {
           let url = format!("{}/cancel", self.statement_url(statement_id));
           self.post::<(), ()>(&url, &()).await
       }

       pub async fn close_statement(&self, statement_id: &str) -> Result<()> {
           self.delete(&self.statement_url(statement_id)).await
       }
   }
   ```

2. **Update Statement cancel()**
   ```rust
   impl Statement for DatabricksStatement {
       fn cancel(&mut self) -> adbc_core::error::Result<()> {
           if let Some(ref stmt_id) = self.statement_id {
               self.runtime.clone()
                   .block_on(self.client.cancel_statement(stmt_id))
                   .map_err(Into::into)?;
               self.statement_id = None;
           }
           Ok(())
       }
   }
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Running statement cancelled | API returns success |
| In-flight fetches stop | ChunkFetcher terminates |
| Statement ID cleared | Can execute new query |

---

# Sprint 4: Metadata APIs & execute_update

## 4.1-4.6 Connection Metadata APIs

### Objective
Implement all Connection metadata methods: get_info, get_table_types, get_objects at all depths.

### Actions (Summary)

For each metadata API, the pattern is:
1. Execute appropriate SQL query (SHOW CATALOGS, SHOW SCHEMAS, etc.)
2. Parse results into ADBC-specified Arrow schema
3. Return as RecordBatchReader

**Key SQL queries:**
- Catalogs: `SHOW CATALOGS`
- Schemas: `SHOW SCHEMAS IN <catalog>`
- Tables: `SHOW TABLES IN <catalog>.<schema>`
- Columns: `DESCRIBE TABLE <catalog>.<schema>.<table>`

**ADBC schema requirements** (from adbc_core::schemas):
- get_info: InfoCode -> Value mapping
- get_objects: Hierarchical catalog -> schema -> table -> column
- get_table_types: Single column "table_type"

---

## 4.7 Connection - get_table_schema()

### Objective
Implement efficient single-table schema retrieval.

### Actions

```rust
impl Connection for DatabricksConnection {
    fn get_table_schema(
        &mut self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        let full_name = format!(
            "{}.{}.{}",
            catalog.unwrap_or("main"),
            db_schema.unwrap_or("default"),
            table_name
        );

        let sql = format!("DESCRIBE TABLE {}", full_name);

        let mut stmt = self.new_statement()?;
        stmt.set_sql_query(&sql)?;
        let reader = stmt.execute()?;

        // Parse DESCRIBE output into Arrow Schema
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;

        self.describe_to_schema(&batches)
    }
}
```

---

## 4.8 Statement - execute_update()

### Objective
Implement DDL/DML execution returning affected row count.

### Actions

```rust
impl Statement for DatabricksStatement {
    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        let reader = self.runtime.clone()
            .block_on(self.execute_async())
            .map_err(Into::into)?;

        // For DDL (CREATE, DROP, ALTER), affected_rows is None
        // For DML (INSERT, UPDATE, DELETE), extract from response
        Ok(reader.affected_rows())
    }
}
```

---

## 4.9 Statement - execute_schema()

### Objective
Get query schema without executing the full query.

### Actions

```rust
impl Statement for DatabricksStatement {
    fn execute_schema(&mut self) -> adbc_core::error::Result<Schema> {
        // Execute with row_limit = 0 to get schema only
        let original_limit = self.config.row_limit;
        self.config.row_limit = Some(0);

        let reader = self.execute()?;
        let schema = reader.schema();

        self.config.row_limit = original_limit;

        Ok((*schema).clone())
    }
}
```

---

# Sprint 5: Testing, Polish & Release Preparation

## 5.1-5.8 Testing & Release Items

### Unit Test Suite
- Mock HTTP responses using wiremock
- Test all error mappings
- Test retry logic timing
- Test type conversions

### Integration Test Suite
- Environment-based configuration
- Skip without credentials
- Test connection lifecycle
- Test query execution (small/large)
- Test metadata APIs

### Connection String Parsing
```rust
// databricks://<host>/<warehouse_id>?token=<pat>&catalog=<cat>&schema=<sch>
pub fn parse_connection_string(uri: &str) -> Result<DatabaseConfig> {
    let url = url::Url::parse(uri)?;
    // Extract components...
}
```

### Environment Variable Support
```rust
impl DatabaseConfig {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("DATABRICKS_HOST").ok(),
            warehouse_id: std::env::var("DATABRICKS_WAREHOUSE_ID").ok(),
            token: std::env::var("DATABRICKS_TOKEN").ok(),
            // ...
        }
    }
}
```

### FFI Export
```rust
// In lib.rs
#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(DatabricksDriverInit, DatabricksDriver);
```

### Documentation
- README with quick start
- rustdoc for all public APIs
- Configuration reference table
- Troubleshooting section

---

# Appendix: File Summary

| File | Sprint | Description |
|------|--------|-------------|
| `Cargo.toml` | 1 | Package configuration |
| `src/lib.rs` | 1 | Public exports, FFI |
| `src/driver.rs` | 1 | DatabricksDriver |
| `src/database.rs` | 1 | DatabricksDatabase |
| `src/connection.rs` | 2 | DatabricksConnection |
| `src/statement.rs` | 2-3 | DatabricksStatement |
| `src/error.rs` | 1 | Error types |
| `src/options.rs` | 1 | Configuration |
| `src/session.rs` | 1 | Session management |
| `src/client/mod.rs` | 1-3 | SeaClient |
| `src/client/models.rs` | 2-3 | Request/Response types |
| `src/fetch/mod.rs` | 3 | ChunkFetcher |
| `src/fetch/reader.rs` | 2-3 | ArrowResultReader |
| `src/fetch/decompress.rs` | 3 | LZ4 decompression |
| `tests/unit/*.rs` | 5 | Unit tests |
| `tests/integration/*.rs` | 5 | Integration tests |
