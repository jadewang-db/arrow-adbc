# Databricks Rust ADBC Driver - Detailed Implementation Plan

**Document Version**: 2.0
**Created**: 2024-12-08
**Last Updated**: 2024-12-15
**Design Reference**: [databricks-rust-adbc-driver-design.md](./databricks-rust-adbc-driver-design.md)

---

## Testing Philosophy

This implementation plan follows a **continuous E2E testing** approach where each work item includes appropriate test coverage:

- **Unit Tests**: Fast, isolated tests for individual functions/components
- **Integration Tests**: Tests for component interactions (may use mocks)
- **E2E Tests**: Tests against real Databricks SQL Warehouse instances (when applicable)

**Key Principle**: E2E tests are introduced as early as possible. Each work item's exit criteria includes E2E tests with a real Databricks instance when the functionality can be meaningfully tested end-to-end.

---

## Table of Contents

1. [Sprint 1: Foundation & Core Infrastructure](#sprint-1-foundation--core-infrastructure)
2. [Sprint 2: Connection & Basic Statement Execution](#sprint-2-connection--basic-statement-execution)
3. [Sprint 3: External Links & Parallel Chunk Fetching](#sprint-3-external-links--parallel-chunk-fetching)
4. [Sprint 4: Metadata APIs & execute_update](#sprint-4-metadata-apis--execute_update)
5. [Sprint 5: Polish & Release Preparation](#sprint-5-polish--release-preparation)

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

### Test Types
- **Unit Tests**: Cargo.toml parsing, dependency resolution
- **Integration Tests**: N/A
- **E2E Tests**: N/A (no runtime functionality yet)

### Expected Results

| Result | Verification | Test Type |
|--------|--------------|-----------|
| Project compiles | `cargo build -p adbc-driver-databricks` succeeds | Build |
| Dependencies resolve | No version conflicts in `Cargo.lock` | Build |
| Module structure created | All files exist with basic module declarations | Build |
| Workspace integration | Driver appears in `cargo workspace` output | Build |

### E2E Exit Criteria
❌ **No E2E tests** - This is project setup only, no runtime functionality to test

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

### Test Types
- **Unit Tests**: Error type conversion, status mapping, retryability checks
- **Integration Tests**: N/A
- **E2E Tests**: N/A (errors tested through actual operations in later sprints)

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

#[test]
fn test_all_sea_error_codes() {
    // Test all error codes from design doc Section 5.2
    let test_cases = vec![
        ("BAD_REQUEST", 400, Status::InvalidArguments, false),
        ("UNAUTHENTICATED", 401, Status::Unauthenticated, true),
        ("PERMISSION_DENIED", 403, Status::Unauthorized, false),
        ("NOT_FOUND", 404, Status::NotFound, false),
        ("REQUEST_LIMIT_EXCEEDED", 429, Status::IO, true),
        ("INTERNAL_ERROR", 500, Status::Internal, true),
        ("TEMPORARILY_UNAVAILABLE", 503, Status::IO, true),
    ];

    for (code, status, expected_adbc, expected_retry) in test_cases {
        let err = Error::SeaApi {
            code: code.into(),
            message: "test".into(),
            http_status: status
        };
        assert_eq!(err.to_adbc_status(), expected_adbc, "Failed for {}", code);
        assert_eq!(err.is_retryable(), expected_retry, "Retry check failed for {}", code);
    }
}
```

### E2E Exit Criteria
❌ **No E2E tests** - Error handling tested through actual operations in subsequent work items

### Files Modified/Created
- `driver/databricks/src/error.rs`
- `driver/databricks/tests/unit/error_tests.rs`

---

## 1.2a E2E Test Configuration Infrastructure

### Objective
Establish the E2E test infrastructure early so all subsequent work items can include E2E tests against real Databricks instances.

### Actions

1. **Create E2E test configuration struct** (`tests/e2e/config.rs`)

   The Rust driver reuses the existing C# ADBC driver test configuration format for consistency. Configuration is loaded from a JSON file specified by `DATABRICKS_TEST_CONFIG_FILE` environment variable.

   ```rust
   use serde::{Deserialize, Serialize};

   /// E2E test configuration matching C# ADBC driver format
   #[derive(Debug, Clone, Serialize, Deserialize)]
   pub struct E2EConfig {
       /// Environment name (e.g., "Databricks")
       pub environment: String,

       /// Full URI including warehouse path
       /// Format: https://{host}/sql/1.0/warehouses/{warehouse_id}
       pub uri: String,

       /// Personal Access Token
       pub token: String,

       /// Optional test query
       #[serde(default)]
       pub query: String,

       /// Driver type (e.g., "databricks")
       #[serde(rename = "type")]
       pub driver_type: String,

       /// Trace logging flag
       #[serde(default)]
       pub trace: String,

       /// Expected number of results for test query
       #[serde(rename = "expectedResults", default)]
       pub expected_results: i64,

       /// Test metadata
       #[serde(default)]
       pub metadata: TestMetadata,
   }

   #[derive(Debug, Clone, Default, Serialize, Deserialize)]
   pub struct TestMetadata {
       /// Catalog name for tests
       pub catalog: String,

       /// Schema name for tests
       pub schema: String,

       /// Table name for tests
       pub table: String,

       /// Expected column count for metadata tests
       #[serde(rename = "expectedColumnCount", default)]
       pub expected_column_count: i32,
   }

   impl E2EConfig {
       /// Load configuration from file specified by DATABRICKS_TEST_CONFIG_FILE
       pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
           let config_path = std::env::var("DATABRICKS_TEST_CONFIG_FILE")?;
           let content = std::fs::read_to_string(config_path)?;
           let config: E2EConfig = serde_json::from_str(&content)?;
           Ok(config)
       }

       /// Parse host and warehouse_id from uri
       pub fn parse_uri(&self) -> Result<(String, String), Box<dyn std::error::Error>> {
           // Parse: https://{host}/sql/1.0/warehouses/{warehouse_id}
           let url = url::Url::parse(&self.uri)?;
           let host = url.host_str().ok_or("No host in URI")?.to_string();
           let warehouse_id = url.path_segments()
               .and_then(|segments| segments.last())
               .ok_or("No warehouse_id in URI")?
               .to_string();
           Ok((format!("https://{}", host), warehouse_id))
       }
   }
   ```

2. **Create test helper functions** (`tests/e2e/helpers.rs`)
   - Lazy-loaded configuration from JSON file
   - Test skip macro when config unavailable
   - Helper functions: `create_test_connection()`, `create_test_database()`

3. **Create example configuration file** (`tests/e2e/databricks.example.json`)
   - Template matching C# format with placeholders
   - Add `*.local.json` to `.gitignore`

   ```json
   {
     "environment": "Databricks",
     "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",
     "token": "dapi...",
     "query": "select count(*) from `main`.`your_schema`.`your_table`",
     "type": "databricks",
     "trace": "true",
     "expectedResults": 1,
     "metadata": {
       "catalog": "main",
       "schema": "your_schema",
       "table": "your_table",
       "expectedColumnCount": 3
     }
   }
   ```

4. **Create test data setup script** (`tests/e2e/setup.sql`)
   - Create test catalog and schema
   - Create `test_types` table with all data types
   - Create `test_large` table for external links testing

5. **Create setup documentation** (`tests/e2e/README.md`)
   - Configuration instructions
   - Test execution commands
   - Troubleshooting guide

### Test Types
- **Unit Tests**: Configuration parsing and validation
- **Integration Tests**: N/A
- **E2E Tests**: Connection to real Databricks warehouse (basic smoke test)

### Expected Results

| Result | Verification | Test Type |
|--------|--------------|-----------|
| Config loads from JSON | Parse example file successfully | Unit |
| Config validates required fields | Missing fields return error | Unit |
| Can connect to warehouse | Basic connection test passes | E2E |
| Test data setup succeeds | Tables created in Databricks | E2E (manual) |

### E2E Exit Criteria
✅ **E2E Test**: `test_e2e_config_and_connect` - Verify configuration loads and can establish connection to real Databricks warehouse

```rust
#[test]
#[ignore]
fn test_e2e_config_and_connect() {
    skip_if_no_config!();
    let config = get_test_config();

    // Verify config loaded from JSON file
    assert!(!config.environment.is_empty());
    assert!(!config.uri.is_empty());
    assert!(!config.token.is_empty());

    // Verify URI parsing works
    let (host, warehouse_id) = config.parse_uri().unwrap();
    assert!(host.starts_with("https://"));
    assert!(!warehouse_id.is_empty());

    println!("Config loaded successfully from DATABRICKS_TEST_CONFIG_FILE");
}
```

### Files Created
- `tests/e2e/config.rs` - Configuration struct matching C# format
- `tests/e2e/helpers.rs` - Test helper functions
- `tests/e2e/databricks.example.json` - Example configuration template
- `tests/e2e/setup.sql` - Test data setup script
- `tests/e2e/README.md` - Setup and execution documentation

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
- `driver/databricks/src/lib.rs` (made `client` module public)
- `driver/databricks/tests/e2e_tests.rs` (added E2E tests)

### Implementation Notes (Completed 2024-12-16)

**Implementation Details:**
- Added `SeaClientConfig` struct with builder pattern for configuration
- Implemented `SeaClient` with `reqwest::Client` for HTTP operations
- Added all URL helper methods: `base_url()`, `statements_url()`, `statement_url()`,
  `statement_cancel_url()`, `chunk_url()`, `sessions_url()`, `session_url()`
- Implemented generic HTTP methods: `post()`, `get()`, `delete()`, `delete_with_params()`
- Error responses are parsed to extract `error_code` and `message` from SEA API
- Default headers include Authorization (Bearer token), Content-Type (application/json),
  and User-Agent (adbc-driver-databricks/<version>)

**API Discovery Notes:**
- DELETE session endpoint requires `warehouse_id` as a query parameter (not documented)
- Implemented `delete_with_params()` to support this pattern

**E2E Tests Added:**
- `test_e2e_sea_client_instantiation`: Validates client creation
- `test_e2e_sea_client_session_lifecycle`: Creates and deletes session against real warehouse
- `test_e2e_sea_client_auth_error`: Validates 401 error handling with invalid token

**Test Results:**
- 34 unit tests pass
- 3 E2E tests pass against real Databricks instance

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

### Test Types
- **Unit Tests**: Session request serialization, response parsing
- **Integration Tests**: Session manager state management (with mock client)
- **E2E Tests**: Session lifecycle with real Databricks warehouse

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

### E2E Exit Criteria
✅ **E2E Test**: `test_e2e_session_create_and_terminate` - Verify session creation and termination with real Databricks warehouse

```rust
#[test]
#[ignore]
fn test_e2e_session_create_and_terminate() {
    skip_if_no_config!();

    let config = get_test_config();
    let client_config = SeaClientConfig {
        host: config.host_name.unwrap(),
        token: config.token.unwrap(),
        warehouse_id: config.warehouse_id().unwrap(),
        connect_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(300),
    };

    let client = Arc::new(SeaClient::new(client_config).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Create session
    let session_id = runtime.block_on(client.create_session(
        config.catalog,
        config.schema
    )).unwrap();

    assert!(!session_id.is_empty());
    println!("Created session: {}", session_id);

    // Terminate session
    runtime.block_on(client.delete_session(&session_id)).unwrap();
    println!("Terminated session: {}", session_id);
}
```

### Files Modified/Created
- `driver/databricks/src/client/models.rs`
- `driver/databricks/src/client/mod.rs` (add session methods)
- `driver/databricks/src/session.rs`
- `tests/e2e/session_tests.rs`

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
- `driver/databricks/src/client/retry.rs` (new module for retry config)
- `driver/databricks/src/error.rs` (add retry_after field to SeaApi error)
- `driver/databricks/Cargo.toml` (add `rand` dependency)

### Implementation Notes (Completed 2024-12-16)

**Implementation Details:**
- Created separate `src/client/retry.rs` module for cleaner organization
- Added `RetryConfig` struct with `Default` implementation and builder methods:
  - `new()` - custom configuration
  - `no_retry()` - disable retries
  - `with_max_retries()` - set only max retries
- Implemented `delay_for_attempt()` with exponential backoff and jitter
- Implemented `delay_with_retry_after()` for server-specified delays
- Added `parse_retry_after()` function for parsing Retry-After header values

**Error Type Enhancement:**
- Added `retry_after: Option<Duration>` field to `Error::SeaApi` variant
- Added `sea_api_with_retry_after()` constructor method
- Added `retry_after()` accessor method on `Error`
- Updated `handle_error_response()` in SeaClient to extract Retry-After header for 429 responses

**Retry Wrapper Implementation:**
- `with_retry()` is a generic async method on SeaClient
- Uses `Error::is_retryable()` to determine if retry should occur
- Prefers server-specified Retry-After delay when available
- Falls back to exponential backoff calculation otherwise

**Test Coverage:**
- 17 unit tests for RetryConfig and backoff calculation
- 6 async tests for with_retry() behavior:
  - Success on first attempt
  - Success after transient failures
  - Retry exhaustion
  - Non-retryable fails immediately
  - Retry-After header respected
  - No-retry config works correctly

**All Exit Criteria Met:**
| Result | Status |
|--------|--------|
| Backoff increases exponentially | Pass - Verified: 1s, 2s, 4s, 8s, 16s |
| Jitter applied | Pass - Delays vary within [base, base*(1+jitter)] |
| Max delay respected | Pass - Capped at configured max_delay |
| 429 uses Retry-After | Pass - Server delay used when available |
| Non-retryable fails fast | Pass - 400 errors don't retry |
| All unit tests pass | Pass - 66 tests passing |

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

### Test Types
- **Unit Tests**: Connection option setting/getting
- **Integration Tests**: Connection lifecycle with mock session manager
- **E2E Tests**: Full connection lifecycle with real Databricks warehouse

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

### E2E Exit Criteria
✅ **E2E Test**: `test_e2e_connection_lifecycle` - Verify complete connection lifecycle with session creation and cleanup

```rust
#[test]
#[ignore]
fn test_e2e_connection_lifecycle() {
    skip_if_no_config!();

    let session_id = {
        let mut conn = create_test_connection();

        // Verify connection is established
        // Note: May need to add public API to check connection status
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("SELECT 1").unwrap();
        let result = stmt.execute();

        // Connection works if query succeeds
        assert!(result.is_ok(), "Connection should be functional");

        // Get session ID before dropping (if API available)
        // conn.session_id().unwrap()
        "session_verified".to_string()
    }; // Connection dropped here, session should be terminated

    assert!(!session_id.is_empty());
    println!("Connection lifecycle verified with session: {}", session_id);
}
```

### Files Modified/Created
- `driver/databricks/src/connection.rs`
- `tests/e2e/connection_tests.rs`

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
       /// Behavior when wait timeout is reached: "CONTINUE" or "CANCEL"
       pub on_wait_timeout: String,
       /// Result disposition: "INLINE" or "EXTERNAL_LINKS"
       pub disposition: String,
       /// Result format: "ARROW_STREAM", "JSON_ARRAY", or "CSV"
       pub format: String,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub row_limit: Option<i64>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub byte_limit: Option<i64>,
   }

   /// Note: The SEA API supports two disposition values:
   /// - "INLINE" - Result data included in response (limited to 25 MiB, JSON_ARRAY format only)
   /// - "EXTERNAL_LINKS" - Result data via presigned URLs (all formats, larger results)
   ///
   /// We default to "EXTERNAL_LINKS" because:
   /// - It works with ARROW_STREAM format (our default)
   /// - It supports larger result sets
   /// - It provides better throughput via Cloud Fetch technology
   ///
   /// Also note: The SEA API does not allow setting session_id at the same time as
   /// catalog or schema fields in the execute statement request.
   impl ExecuteStatementRequest {
       fn new(warehouse_id: String, statement: String) -> Self {
           Self {
               statement,
               warehouse_id,
               session_id: None,
               catalog: None,
               schema: None,
               wait_timeout: "10s".to_string(),
               on_wait_timeout: "CONTINUE".to_string(),
               disposition: "EXTERNAL_LINKS".to_string(),
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
    let request = ExecuteStatementRequest::new("abc123", "SELECT 1")
        .with_session_id("session456");

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("ARROW_STREAM"));
    assert!(json.contains("EXTERNAL_LINKS"));
    assert!(json.contains("on_wait_timeout"));
}

#[test]
fn test_execute_response_deserialization() {
    let json = r#"{
        "statement_id": "stmt123",
        "status": { "state": "SUCCEEDED" },
        "result": { "chunk_index": 0, "row_count": 1 }
    }"#;

    let response: StatementResponse = serde_json::from_str(json).unwrap();
    assert_eq!(response.statement_id, "stmt123");
    assert_eq!(response.status.state, StatementState::Succeeded);
}
```

### Implementation Notes (from Work Item 2.3)
- Added `on_wait_timeout` field to ExecuteStatementRequest for controlling behavior when wait timeout is reached
- Changed default disposition from "INLINE_OR_EXTERNAL_LINKS" (invalid) to "EXTERNAL_LINKS"
- Added builder pattern with `with_*` methods for ExecuteStatementRequest
- SEA API restriction: Cannot set session_id at same time as catalog/schema in execute request
- Comprehensive wiremock tests added for all response states and error handling
- E2E tests verified against real Databricks SQL warehouse

### Files Modified/Created
- `driver/databricks/src/client/models.rs`
- `driver/databricks/src/client/mod.rs` (add execute_statement)

---

## 2.4 SEA Client - Statement Polling

### Objective
Implement statement status polling with exponential backoff for async statement execution.

### Implementation Status: COMPLETED

### Actions

1. **Implemented get_statement in SeaClient** (already existed from 2.3)
   ```rust
   impl SeaClient {
       /// Get statement status and results
       pub async fn get_statement(&self, statement_id: &str) -> Result<StatementResponse> {
           self.get(&self.statement_url(statement_id)).await
       }
   }
   ```

2. **Implemented poll_until_complete with inline configuration**
   ```rust
   impl SeaClient {
       /// Poll statement until completion with exponential backoff
       /// - Initial delay: 1 second
       /// - Max delay: 10 seconds
       /// - Backoff multiplier: 2x
       /// - Default timeout: 5 minutes
       pub async fn poll_until_complete(
           &self,
           statement_id: &str,
           max_wait: Option<Duration>,
       ) -> Result<StatementResponse> {
           let start = std::time::Instant::now();
           let max_wait = max_wait.unwrap_or(Duration::from_secs(300));

           let mut poll_interval = Duration::from_secs(1);
           let max_poll_interval = Duration::from_secs(10);

           loop {
               let response = self.get_statement(statement_id).await?;

               match response.status.state {
                   StatementState::Succeeded => return Ok(response),
                   StatementState::Failed => {
                       let error_msg = response.status.error
                           .as_ref()
                           .map(|e| format!("{}: {}",
                               e.error_code.as_deref().unwrap_or("UNKNOWN"),
                               e.message.as_deref().unwrap_or("Statement failed")))
                           .unwrap_or_else(|| "Statement failed".to_string());
                       return Err(Error::statement_failed(error_msg));
                   }
                   StatementState::Canceled => {
                       return Err(Error::statement_failed("Statement was canceled"));
                   }
                   StatementState::Closed => {
                       return Err(Error::statement_failed("Statement was closed"));
                   }
                   StatementState::Pending | StatementState::Running => {
                       if start.elapsed() >= max_wait {
                           return Err(Error::Timeout);
                       }
                       tokio::time::sleep(poll_interval).await;
                       poll_interval = (poll_interval * 2).min(max_poll_interval);
                   }
               }
           }
       }
   }
   ```

3. **Implemented execute_and_wait helper**
   ```rust
   impl SeaClient {
       /// Execute a SQL statement and wait for completion.
       /// Note: The SEA API does not allow setting session_id and catalog/schema
       /// at the same time. Catalog/schema should be set on the session instead.
       pub async fn execute_and_wait(
           &self,
           session_id: &str,
           sql: &str,
           max_wait: Option<Duration>,
           row_limit: Option<i64>,
           byte_limit: Option<i64>,
       ) -> Result<StatementResponse> {
           // Execute with 10s initial wait for fast queries
           let mut request = ExecuteStatementRequest::new(&self.warehouse_id, sql)
               .with_session_id(session_id)
               .with_wait_timeout("10s");

           if let Some(limit) = row_limit {
               request = request.with_row_limit(limit);
           }
           if let Some(limit) = byte_limit {
               request = request.with_byte_limit(limit);
           }

           let response = self.execute_statement(&request).await?;

           match response.status.state {
               StatementState::Succeeded => Ok(response),
               StatementState::Failed | StatementState::Canceled | StatementState::Closed => {
                   // Return error with details
                   Err(Error::statement_failed(...))
               }
               StatementState::Pending | StatementState::Running => {
                   // Poll until complete
                   self.poll_until_complete(&response.statement_id, max_wait).await
               }
           }
       }
   }
   ```

### Important API Constraint
**SEA API does not allow combining `session_id` with `catalog`/`schema` in execute_statement**.
- If you need catalog/schema context, create the session with `catalog` and `schema` fields set
- Then use `execute_and_wait` without passing catalog/schema parameters

### Expected Results

| Result | Verification |
|--------|--------------|
| Polling respects delay | First delay ~1s, doubles each iteration |
| Max delay capped | Never exceeds 10s between polls |
| Timeout works | Returns Timeout error after specified duration |
| Failed state handled | Returns error with error code and message |
| Canceled state handled | Returns StatementFailed error |
| Closed state handled | Returns StatementFailed error |

### Unit Tests (All Passing)
- `test_get_statement_success` - Basic GET statement status
- `test_get_statement_pending` - Statement in PENDING state
- `test_get_statement_running` - Statement in RUNNING state
- `test_get_statement_failed` - Statement with error details
- `test_poll_until_complete_immediate_success` - First poll returns SUCCEEDED
- `test_poll_until_complete_after_pending` - PENDING -> RUNNING -> SUCCEEDED with backoff timing
- `test_poll_until_complete_fails` - Statement transitions to FAILED
- `test_poll_until_complete_canceled` - Statement transitions to CANCELED
- `test_poll_until_complete_closed` - Statement transitions to CLOSED
- `test_poll_until_complete_timeout` - Always RUNNING, verifies timeout error
- `test_execute_and_wait_immediate_success` - Execute returns SUCCEEDED immediately
- `test_execute_and_wait_with_polling` - Execute returns PENDING, polling completes
- `test_execute_and_wait_fails_immediately` - Execute returns FAILED immediately
- `test_execute_and_wait_fails_during_polling` - Statement fails during polling
- `test_execute_and_wait_canceled_immediately` - Execute returns CANCELED
- `test_execute_and_wait_with_options` - Verify row_limit and byte_limit pass correctly

### E2E Tests (All Passing)
- `test_e2e_get_statement` - Verify GET statement status against real Databricks
- `test_e2e_poll_until_complete` - Poll a query until completion
- `test_e2e_execute_and_wait` - Execute simple and larger queries
- `test_e2e_execute_and_wait_with_catalog_schema` - Create session with catalog/schema, execute query
- `test_e2e_poll_until_complete_failed_statement` - Verify failed statement error handling

### Files Modified/Created
- `driver/databricks/src/client/mod.rs` (add poll_until_complete, execute_and_wait)
- `driver/databricks/tests/e2e_tests.rs` (add Work Item 2.4 E2E tests)

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
- `driver/databricks/src/runtime.rs` (new - async/sync bridge utilities)
- `driver/databricks/src/connection.rs` (updated to use runtime utilities)

### Implementation Notes (Completed 2024-12-16)

**Actual Implementation:**

The async/sync bridge was implemented in a dedicated `runtime` module with three utility functions:

1. **`block_on_async<F, T>(runtime, future) -> Result<T>`**
   - For operations returning `Result<T>`
   - Used by `execute()`, `execute_update()`, `cancel()`
   - Panics if called from async task context (tokio's built-in behavior)

2. **`block_on_async_simple<F, T>(runtime, future) -> Option<T>`**
   - For operations returning plain values
   - Used by `session_id()` to check `is_active()`
   - Returns `Some(value)` in sync context

3. **`block_on_async_or_spawn<F, T>(runtime, future) -> Option<Result<T>>`**
   - For `Drop` implementations
   - Detects async context using `Handle::try_current()`
   - In async context: spawns detached task, returns `None`
   - In sync context: blocks and returns `Some(result)`

**Key Design Decision:**

Initially we tried to detect nested runtime context using `Handle::try_current().is_ok()`,
but this check is too strict - it returns true even inside `spawn_blocking` threads where
`block_on` is actually safe. The final implementation:
- For normal operations: trusts tokio's built-in panic behavior
- For Drop: uses `Handle::try_current()` to decide between blocking vs spawning

**Tests Added:**
- Unit tests in `runtime.rs` for all three functions
- E2E tests for full workflow, multiple statements, execute_update, and drop cleanup

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

## 2.8 Statement Execute - External Links Path

### Objective
Complete the execute() method for queries returning results via external links (presigned URLs).

> **Note**: The SEA API requires `EXTERNAL_LINKS` disposition when using `ARROW_STREAM` format. The `INLINE` disposition only supports `JSON_ARRAY` format. Therefore, all Arrow data is returned via presigned cloud storage URLs, even for small result sets.

### Implementation (Completed)

1. **ChunkFetcher for external links fetching**
   ```rust
   impl ChunkFetcher {
       pub async fn fetch_chunks(
           &self,
           _statement_id: &str,
           _manifest: &ResultManifest,
           external_links: &[ExternalLink],
       ) -> Result<Vec<RecordBatch>> {
           let mut all_batches = Vec::new();

           for link in external_links {
               let bytes = self.download_chunk(link).await?;
               if bytes.is_empty() {
                   continue;
               }
               let (_, batches) = ArrowResultReader::parse_ipc_stream(&bytes)?;
               all_batches.extend(batches);
           }

           Ok(all_batches)
       }

       pub async fn download_chunk(&self, link: &ExternalLink) -> Result<Vec<u8>> {
           let mut request = self.http_client.get(&link.external_link);

           if let Some(ref headers) = link.http_headers {
               for (key, value) in headers {
                   request = request.header(key, value);
               }
           }

           let response = request.send().await.map_err(crate::error::Error::Http)?;
           // ... error handling and return bytes
       }
   }
   ```

2. **response_to_reader in DatabricksStatement**
   ```rust
   fn response_to_reader(&self, response: StatementResponse) -> Result<ArrowResultReader> {
       let schema = self.build_schema_from_response(&response)?;

       // Check for external links (primary path for ARROW_STREAM)
       if let Some(ref external_links) = result.external_links {
           if !external_links.is_empty() {
               let fetcher = ChunkFetcher::new(self.client.clone(), 1)?;
               let batches = block_on_async(&self.runtime, async move {
                   fetcher.fetch_chunks(statement_id, &manifest, &links).await
               })?;
               return Ok(ArrowResultReader::new(schema, batches));
           }
       }

       // No data - return empty reader
       Ok(ArrowResultReader::empty(schema))
   }
   ```

### Test Types
- **Unit Tests**: Schema conversion, Arrow IPC parsing (with mock data)
- **Integration Tests**: Query execution with mocked HTTP responses
- **E2E Tests**: Full query execution against real Databricks (inline results)

### Expected Results

| Result | Verification | Test Type |
|--------|--------------|-----------|
| Simple SELECT works | Returns expected data | E2E |
| Schema correct | Column names and types match | E2E |
| Data parseable | RecordBatch iteration succeeds | E2E |
| Empty results handled | Zero-row queries work | E2E |

### E2E Exit Criteria (All Passing)

The following E2E tests validate Work Item 2.8:
- `test_e2e_execute_inline_select_simple` - SELECT 1 returns actual data
- `test_e2e_execute_inline_multiple_columns` - Multiple columns work
- `test_e2e_execute_inline_multiple_rows` - Multiple rows work
- `test_e2e_execute_inline_null_values` - NULL values handled correctly
- `test_e2e_execute_inline_all_types` - Various data types work
- `test_e2e_execute_inline_comprehensive` - Full end-to-end workflow

✅ **E2E Test**: `test_e2e_query_select_basic` - Execute simple queries and verify results with real Databricks

```rust
#[test]
#[ignore]
fn test_e2e_query_select_basic() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Test 1: SELECT 1
    stmt.set_sql_query("SELECT 1 AS one").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "one");

    let array = batch.column(0).as_any()
        .downcast_ref::<Int32Array>().unwrap();
    assert_eq!(array.value(0), 1);

    // Test 2: SELECT with multiple columns
    stmt.set_sql_query("SELECT 42 AS num, 'hello' AS msg").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);

    // Test 3: Empty result
    stmt.set_sql_query("SELECT * FROM range(0, 0)").unwrap();
    let reader = stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    println!("Basic query execution verified");
}
```

✅ **E2E Test**: `test_e2e_query_data_types` - Verify all basic data types work correctly

```rust
#[test]
#[ignore]
fn test_e2e_query_data_types() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query(
        "SELECT \
         true AS bool_col, \
         CAST(127 AS TINYINT) AS tinyint_col, \
         CAST(32767 AS SMALLINT) AS smallint_col, \
         CAST(2147483647 AS INT) AS int_col, \
         CAST(9223372036854775807 AS BIGINT) AS bigint_col, \
         CAST(3.14 AS FLOAT) AS float_col, \
         CAST(2.718281828 AS DOUBLE) AS double_col, \
         'Hello, World!' AS string_col"
    ).unwrap();

    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_columns(), 8);
    assert_eq!(batch.num_rows(), 1);

    // Verify boolean
    let bool_col = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(bool_col.value(0), true);

    // Verify integers
    let int_col = batch.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_col.value(0), 2147483647);

    // Verify string
    let string_col = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(string_col.value(0), "Hello, World!");

    println!("Data type handling verified");
}
```

### Files Modified/Created
- `driver/databricks/src/statement.rs` (add create_reader_from_result)
- `driver/databricks/tests/integration/basic_query.rs`
- `tests/e2e/query_basic_tests.rs`

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

## 3.4 ChunkFetcher - Worker Pool Pattern

### Objective
Implement a worker pool that continuously pulls chunks from an ordered queue and downloads them, maintaining a fixed number of concurrent downloads.

### Actions

1. **Implement worker pool-based chunk fetching**

   This approach maintains a fixed number of worker tasks that continuously pull from a queue, providing better resource management and predictable concurrency.

   ```rust
   use tokio::sync::mpsc;
   use std::sync::Arc;

   impl ChunkFetcher {
       /// Fetch chunks using worker pool pattern
       /// Workers continuously pull from queue in order (0, 1, 2, ...)
       ///
       /// IMPORTANT: Workers are NEVER blocked by:
       /// 1. Ordering logic (runs in separate task via channels)
       /// 2. Slow downloads (each worker is independent)
       /// 3. URL refresh (handled per-worker, doesn't affect others)
       pub async fn fetch_chunks_ordered(
           &self,
           links: Vec<ExternalLink>,
       ) -> Result<impl Stream<Item = Result<Vec<u8>>>> {
           let total_chunks = links.len();
           let num_workers = self.concurrency.min(total_chunks);

           // Create channels with buffering to prevent blocking
           // Buffer size = num_workers * 2 allows workers to continue without waiting
           let (work_tx, work_rx) = mpsc::channel::<ExternalLink>(num_workers * 2);
           let (result_tx, result_rx) = mpsc::channel::<(usize, Result<Vec<u8>>)>(num_workers * 2);

           // Sort links by chunk_index to ensure ordered queue feeding
           let mut sorted_links = links;
           sorted_links.sort_by_key(|link| link.chunk_index);

           // Spawn work producer (feeds chunks in order to queue)
           let work_producer = tokio::spawn(async move {
               for link in sorted_links {
                   if work_tx.send(link).await.is_err() {
                       break; // Receiver dropped
                   }
               }
           });

           // Spawn independent worker pool
           // Key: Each worker is INDEPENDENT and NEVER waits for other workers
           let fetcher = Arc::new(self.clone());
           let mut worker_handles = Vec::new();

           for worker_id in 0..num_workers {
               let mut work_rx = work_rx.clone();
               let result_tx = result_tx.clone();
               let fetcher = fetcher.clone();

               let handle = tokio::spawn(async move {
                   // Worker loop: pull → download → send result → repeat
                   // NEVER blocked by other workers or ordering logic
                   while let Some(link) = work_rx.recv().await {
                       let chunk_index = link.chunk_index as usize;

                       // fetch_chunk_with_retry handles URL refresh internally
                       // If URL expires, THIS worker refreshes and retries
                       // Other workers continue unaffected
                       let result = fetcher.fetch_chunk_with_retry(&link).await;

                       // Send to buffered channel (non-blocking with capacity)
                       if result_tx.send((chunk_index, result)).await.is_err() {
                           break; // Receiver dropped
                       }

                       // Immediately pull next chunk from queue!
                       // No waiting for ordering or other workers
                   }
               });

               worker_handles.push(handle);
           }

           // Drop original senders so workers can complete
           drop(work_tx);
           drop(result_tx);

           // Create ordered output stream
           // This runs in SEPARATE task, doesn't block workers
           let ordered_stream = Self::create_ordered_stream(
               result_rx,
               total_chunks,
               work_producer,
               worker_handles
           );

           Ok(ordered_stream)
       }

       /// Create a stream that yields chunks in order
       ///
       /// Key insight: Workers download at different speeds, so chunks complete out of order.
       /// This function ensures output is always sequential (0, 1, 2, ...) by:
       /// 1. Buffering out-of-order chunks
       /// 2. Only yielding when the next expected chunk is ready
       ///
       /// Example: If chunks complete in order [2, 0, 3, 1, 4]:
       ///   - Chunk 2 arrives → buffer[2] = data, wait (need chunk 0 first)
       ///   - Chunk 0 arrives → buffer[0] = data, yield 0 immediately
       ///   - Chunk 3 arrives → buffer[3] = data, wait (need chunk 1)
       ///   - Chunk 1 arrives → buffer[1] = data, yield 1, then yield 2, then yield 3
       ///   - Chunk 4 arrives → buffer[4] = data, yield 4
       ///   Result: Output order is always 0, 1, 2, 3, 4
       fn create_ordered_stream(
           mut result_rx: mpsc::Receiver<(usize, Result<Vec<u8>>)>,
           total_chunks: usize,
           work_producer: tokio::task::JoinHandle<()>,
           worker_handles: Vec<tokio::task::JoinHandle<()>>,
       ) -> impl Stream<Item = Result<Vec<u8>>> {
           async_stream::stream! {
               // Buffer to hold chunks until we can yield them in order
               let mut buffer: Vec<Option<Result<Vec<u8>>>> = vec![None; total_chunks];
               let mut next_to_yield = 0;  // Next chunk index we need to output
               let mut received_count = 0;

               // Receive results from workers (may arrive out of order)
               while let Some((index, result)) = result_rx.recv().await {
                   // Store this chunk in the buffer
                   buffer[index] = Some(result);
                   received_count += 1;

                   // Try to yield all consecutive chunks that are now ready
                   // This handles cascading yields when a missing chunk arrives
                   while next_to_yield < total_chunks {
                       if let Some(result) = buffer[next_to_yield].take() {
                           yield result;  // Yield in order!
                           next_to_yield += 1;
                       } else {
                           break; // This chunk not ready yet, wait for it
                       }
                   }

                   if received_count == total_chunks {
                       break;  // All chunks received
                   }
               }

               // Yield any remaining buffered chunks
               while next_to_yield < total_chunks {
                   if let Some(result) = buffer[next_to_yield].take() {
                       yield result;
                       next_to_yield += 1;
                   } else {
                       yield Err(Error::StatementFailed(
                           format!("Missing chunk {}", next_to_yield).into()
                       ));
                       break;
                   }
               }

               // Wait for all tasks to complete
               let _ = work_producer.await;
               for handle in worker_handles {
                   let _ = handle.await;
               }
           }
       }
   }
   ```

2. **How ordering works despite variable download speeds:**

   Workers download at different speeds, so chunks complete out of order. The ordered buffer ensures sequential output:

   ```
   Visual Example:
   ===============
   Chunks needed: [0, 1, 2, 3, 4]

   Time 1: Chunk 2 completes (fast connection)
      buffer: [None, None, Some(2), None, None]
      next_to_yield: 0
      Action: Buffer chunk 2, wait for chunk 0
      Output: (nothing yet)

   Time 2: Chunk 0 completes
      buffer: [Some(0), None, Some(2), None, None]
      next_to_yield: 0
      Action: Yield chunk 0, next_to_yield = 1
      Output: 0

   Time 3: Chunk 3 completes
      buffer: [None, None, Some(2), Some(3), None]
      next_to_yield: 1
      Action: Buffer chunk 3, wait for chunk 1
      Output: (nothing yet)

   Time 4: Chunk 1 completes
      buffer: [None, Some(1), Some(2), Some(3), None]
      next_to_yield: 1
      Action: Yield 1, then cascade yield 2, then cascade yield 3!
      Output: 1, 2, 3 (three chunks yielded consecutively)
      next_to_yield: 4

   Time 5: Chunk 4 completes
      buffer: [None, None, None, None, Some(4)]
      next_to_yield: 4
      Action: Yield chunk 4
      Output: 4

   Final output order: 0, 1, 2, 3, 4 ✓
   (Even though completion order was: 2, 0, 3, 1, 4)
   ```

   **Key mechanism:**
   - Each chunk tagged with its index when sent to workers
   - Results arrive as `(chunk_index, data)` pairs
   - Buffer stores at `buffer[chunk_index]`
   - Only yield `buffer[next_to_yield]` when available
   - Cascade yielding when a blocking chunk arrives

3. **Non-blocking architecture ensures workers never wait:**

   ```
   Architecture Separation:
   ========================

   ┌─────────────────────────────────────────────────────────┐
   │            Work Queue (FIFO, Buffered)                  │
   │         Chunks: 0 → 1 → 2 → 3 → 4 → ...                │
   └──────────────────┬──────────────────────────────────────┘
                      │ Pull next (non-blocking)
        ┌─────────────┼─────────────┬─────────────┐
        ▼             ▼             ▼             ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
   │Worker 1 │  │Worker 2 │  │Worker 3 │  │Worker 4 │  Independent!
   │         │  │         │  │         │  │         │
   │Download │  │Download │  │Download │  │Download │  Each worker:
   │ Chunk 0 │  │ Chunk 1 │  │ Chunk 2 │  │ Chunk 3 │  - Pulls chunk
   │         │  │         │  │         │  │         │  - Downloads
   │(may     │  │(may     │  │(may     │  │(may     │  - Handles retry/refresh
   │ retry/  │  │ retry/  │  │ retry/  │  │ retry/  │  - Sends result
   │ refresh)│  │ refresh)│  │ refresh)│  │ refresh)│  - Pulls next
   └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
        │            │            │            │
        │ Send (index, data) to buffered channel
        └────────────┼────────────┼────────────┘
                     ▼            ▼
        ┌──────────────────────────────────────┐
        │   Result Channel (Buffered)          │
        │   Capacity: num_workers * 2          │
        └──────────────┬───────────────────────┘
                       │ Receive (non-blocking)
                       ▼
        ┌──────────────────────────────────────┐
        │   Ordered Buffer (Separate Task)     │  Doesn't block
        │   buffer[0] buffer[1] buffer[2] ...  │  workers!
        │   Yields: 0 → 1 → 2 → 3 → ...       │
        └──────────────────────────────────────┘
   ```

   **Why workers never block:**

   a) **Workers ↔ Queue**: MPSC channel with buffer (num_workers * 2)
      - Workers pull from queue asynchronously
      - If queue empty, worker waits, but doesn't block others

   b) **Workers ↔ Result Channel**: MPSC channel with buffer (num_workers * 2)
      - Workers send results to channel asynchronously
      - Channel buffer prevents blocking when sending
      - Each worker immediately pulls next chunk after sending

   c) **URL Refresh**: Shared cache with non-blocking refresh
      - Worker refreshing URL doesn't block other workers
      - Refreshed URLs stored in shared cache (Arc<RwLock<HashMap>>)
      - One refresh API call can return multiple URLs → all cached
      - Other workers check cache first, reuse cached URLs
      - Workers continue downloading while one worker refreshes

   d) **Ordering Logic**: Separate async task
      - Reads from result channel in separate task
      - Workers never interact with ordering logic
      - Complete decoupling via channels

   **Example 1: Worker with slow download doesn't block others:**
   ```
   Time 0: All workers pull chunks [0, 1, 2, 3]
   Time 1: Worker 1 (chunk 0) hits slow network, still downloading...
           Worker 2 (chunk 1) completes, pulls chunk 4
           Worker 3 (chunk 2) completes, pulls chunk 5
           Worker 4 (chunk 3) completes, pulls chunk 6
   Time 2: Worker 1 still downloading chunk 0... (doesn't block others!)
           Worker 2 completes chunk 4, pulls chunk 7
           Worker 3 completes chunk 5, pulls chunk 8
           ...
   ```

   **Example 2: URL refresh with cache sharing doesn't block other workers:**
   ```
   Time 0: All workers pull chunks [0, 1, 2, 3]
   Time 1: Worker 1 (chunk 0) gets 403 Forbidden (URL expired!)
           Worker 1: Check cache (miss) → Call refresh_chunk_links(0)...
           Worker 2 (chunk 1) completes successfully, pulls chunk 4
           Worker 3 (chunk 2) completes successfully, pulls chunk 5
           Worker 4 (chunk 3) completes successfully, pulls chunk 6
   Time 2: Worker 1 gets refreshed URLs back: [0, 1, 2, 3, 4, 5]
           Worker 1: Cache ALL 6 URLs, retry download with URL for chunk 0
           Worker 2 completes chunk 4, pulls chunk 7
           Worker 3 completes chunk 5, pulls chunk 8
           Worker 4 completes chunk 6, pulls chunk 9
   Time 3: Worker 1 completes chunk 0 with refreshed URL!
           Worker 1 pulls chunk 10
           Worker 2 (chunk 7) gets 403 Forbidden
           Worker 2: Check cache (HIT for chunk 7!) → Use cached URL, no API call!
           (All workers continue without interruption)
   ```

   **Key insight**: Workers check cache first, refresh is shared:
   ```rust
   async fn fetch_chunk_with_retry(&self, link: &ExternalLink) -> Result<Vec<u8>> {
       // Step 1: Try with original URL
       match self.fetch_chunk(link).await {
           Ok(data) => return Ok(data),
           Err(Error::UrlExpired(chunk_index)) => {
               // Step 2: Check shared cache first
               if let Some(cached) = self.get_cached_url(chunk_index).await {
                   return self.fetch_chunk(&cached).await;
               }
               // Step 3: Cache miss → refresh and cache ALL returned URLs
               let refreshed_urls = self.refresh_chunk_links(chunk_index).await?;
               self.cache_all_urls(refreshed_urls).await;
               // Other workers can now reuse these cached URLs!
           }
       }
   }
   ```

4. **Key advantages of worker pool pattern:**
   - **Fixed concurrency**: Always exactly `num_workers` tasks running (not all chunks at once)
   - **Queue-based**: Chunks pulled from queue in order (0, 1, 2, ...) but may complete in any order
   - **Memory efficient**: Only buffers out-of-order chunks, not all chunks
   - **Better resource usage**: Controlled number of HTTP connections
   - **Scalable**: Works efficiently even with thousands of chunks
   - **Guaranteed order**: Output always sequential regardless of completion order
   - **Never blocks**: Workers, ordering, and URL refresh all independent

5. **Add dependency for async streams**
   ```toml
   # Cargo.toml
   [dependencies]
   async-stream = "0.3"
   ```

### Expected Results

| Result | Verification |
|--------|--------------|
| Chunks yielded in order | Index 0, 1, 2... sequential output |
| Fixed worker count | Exactly `num_workers` concurrent downloads |
| Queue-based processing | Workers continuously pull from ordered queue |
| Workers never blocked | Slow worker doesn't affect others |
| URL refresh non-blocking | Worker refreshing URL doesn't block other workers |
| Ordering non-blocking | Buffering logic doesn't block workers |
| Efficient for large sets | Handles 1000+ chunks without resource issues |
| Memory bounded | Only buffers out-of-order chunks, not all chunks |
| Missing chunk fails | Error if any chunk missing |

---

## 3.5 ChunkFetcher - URL Expiration Handling with Shared Cache

### Objective
Implement automatic URL refresh when presigned URLs expire during download. Handle the case where the refresh API returns multiple refreshed URLs at once.

### Key Challenge
When calling `get_chunk(statement_id, chunk_index)` to refresh an expired URL, the API may return **multiple** refreshed URLs (not just the one requested). We need to:
1. Cache all returned URLs so other workers can reuse them
2. Prevent multiple workers from calling refresh API for the same URL
3. Ensure workers check cache before making refresh requests

### Actions

1. **Add shared URL cache to ChunkFetcher**
   ```rust
   use std::sync::Arc;
   use tokio::sync::RwLock;
   use std::collections::HashMap;

   pub struct ChunkFetcher {
       http_client: Client,
       sea_client: Arc<SeaClient>,
       statement_id: String,
       concurrency: usize,

       // Shared cache of refreshed URLs
       // Key: chunk_index, Value: refreshed ExternalLink
       refreshed_urls: Arc<RwLock<HashMap<i32, ExternalLink>>>,
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
               refreshed_urls: Arc::new(RwLock::new(HashMap::new())),
           })
       }
   }
   ```

2. **Implement fetch with retry and shared cache**
   ```rust
   impl ChunkFetcher {
       async fn fetch_chunk_with_retry(&self, link: &ExternalLink) -> Result<Vec<u8>> {
           const MAX_RETRIES: u32 = 3;
           let mut current_link = link.clone();

           for attempt in 0..MAX_RETRIES {
               match self.fetch_chunk(&current_link).await {
                   Ok(data) => return Ok(data),

                   Err(Error::UrlExpired(chunk_index)) => {
                       // Step 1: Check cache for refreshed URL
                       {
                           let cache = self.refreshed_urls.read().await;
                           if let Some(cached_link) = cache.get(&chunk_index) {
                               current_link = cached_link.clone();
                               continue; // Retry with cached URL
                           }
                       }

                       // Step 2: Cache miss, call refresh API
                       // Note: Multiple workers might reach here, but that's OK
                       // The API call is idempotent and returns same URLs
                       let refreshed_links = self.refresh_chunk_links(chunk_index).await?;

                       // Step 3: Update cache with ALL returned URLs
                       {
                           let mut cache = self.refreshed_urls.write().await;
                           for refreshed_link in refreshed_links {
                               cache.insert(refreshed_link.chunk_index, refreshed_link);
                           }
                       }

                       // Step 4: Get our refreshed URL from cache
                       {
                           let cache = self.refreshed_urls.read().await;
                           current_link = cache.get(&chunk_index)
                               .ok_or_else(|| Error::StatementFailed(
                                   "Refreshed URL not found".into()
                               ))?
                               .clone();
                       }

                       // Retry with refreshed URL
                       continue;
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

       /// Refresh URLs by calling get_chunk API
       /// Returns ALL external links returned by the API (may be multiple)
       async fn refresh_chunk_links(&self, chunk_index: i32) -> Result<Vec<ExternalLink>> {
           let response = self.sea_client
               .get_chunk(&self.statement_id, chunk_index)
               .await?;

           // API may return multiple refreshed URLs, not just the one we asked for
           Ok(response.external_links)
       }
   }
   ```

3. **How it works with multiple workers:**

   ```
   Scenario: 3 workers hit expired URLs at the same time
   ================================================================

   Time 1: Worker 1 downloads chunk 5 → 403 Forbidden (URL expired)
           Worker 2 downloads chunk 8 → 403 Forbidden (URL expired)
           Worker 3 downloads chunk 12 → 403 Forbidden (URL expired)

   Time 2: All 3 workers check cache:
           Worker 1: Cache miss for chunk 5
           Worker 2: Cache miss for chunk 8
           Worker 3: Cache miss for chunk 12

   Time 3: Workers call refresh API (may happen concurrently):
           Worker 1: get_chunk(statement_id, 5)
              ↳ API returns: [link_5, link_6, link_7, link_8, link_9, link_10]
              ↳ Worker 1 caches ALL 6 URLs

           Worker 2: get_chunk(statement_id, 8)
              ↳ API returns: [link_8, link_9, link_10, link_11, link_12]
              ↳ Worker 2 caches ALL 5 URLs

           Worker 3: get_chunk(statement_id, 12)
              ↳ Checks cache first → HIT! (Worker 2 already cached link_12)
              ↳ Uses cached URL, no API call needed!

   Time 4: All workers retry with refreshed URLs:
           Worker 1: Downloads chunk 5 with new URL ✓
           Worker 2: Downloads chunk 8 with new URL ✓
           Worker 3: Downloads chunk 12 with cached URL ✓

   Time 5: Later, Worker 4 encounters chunk 6 expired:
           Worker 4: Checks cache → HIT! (Worker 1 already cached it)
           Worker 4: Uses cached URL, no API call ✓
   ```

   **Benefits:**
   - **Reduces API calls**: Cache reuse when API returns multiple URLs
   - **No coordination needed**: Workers independently check cache
   - **Race condition safe**: Multiple workers calling refresh is OK (idempotent)
   - **Memory efficient**: Only stores refreshed URLs (not original ones)

### Expected Results

| Result | Verification |
|--------|--------------|
| Expired URL refreshed | get_chunk called on 403 |
| Multiple URLs cached | Cache contains all returned links |
| Cache reuse works | Worker finds cached URL, avoids API call |
| Retry with new URL | Download succeeds after refresh |
| Max retries respected | Fails after 3 attempts |
| Thread-safe cache | RwLock prevents data races |

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

### Test Types
- **Unit Tests**: Chunk ordering logic, decompression
- **Integration Tests**: Streaming with mock chunks
- **E2E Tests**: Large query results with external links from real Databricks

### Expected Results

| Result | Verification | Test Type |
|--------|--------------|-----------|
| Large queries work | >1GB results stream correctly | E2E |
| Chunks decompressed | LZ4 data handled | E2E |
| Memory efficient | Constant memory usage | E2E |
| All rows returned | Row count matches manifest | E2E |

### E2E Exit Criteria
✅ **E2E Test**: `test_e2e_query_large_result` - Execute large query and verify external links path with real Databricks

```rust
#[test]
#[ignore]
fn test_e2e_query_large_result() {
    skip_if_no_config!();

    let config = get_test_config();
    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Query the pre-created large table (1M rows)
    stmt.set_sql_query(&format!(
        "SELECT * FROM {}.{}.test_large",
        config.metadata.catalog,
        config.metadata.schema
    )).unwrap();

    let reader = stmt.execute().unwrap();
    let mut total_rows = 0;
    let mut batch_count = 0;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
        batch_count += 1;

        // Verify batch has expected schema
        assert_eq!(batch.num_columns(), 3); // id, text, random_value
    }

    // Verify all rows retrieved
    assert_eq!(total_rows, 1_000_000, "Should retrieve all 1M rows");
    assert!(batch_count > 1, "Should have multiple batches (external links)");

    println!("Large result verified: {} rows in {} batches", total_rows, batch_count);
}
```

✅ **E2E Test**: `test_e2e_query_external_links_decompression` - Verify LZ4 decompression works with real data

```rust
#[test]
#[ignore]
fn test_e2e_query_external_links_decompression() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Generate large result to force external links with compression
    stmt.set_sql_query(
        "SELECT \
         id, \
         CONCAT('row_', CAST(id AS STRING), '_', REPEAT('x', 100)) AS large_text \
         FROM range(0, 100000)"
    ).unwrap();

    let reader = stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 100_000);

    // Verify data integrity
    let first_batch = &batches[0];
    let text_col = first_batch.column(1).as_any()
        .downcast_ref::<StringArray>().unwrap();

    // Check that text contains expected pattern
    let first_value = text_col.value(0);
    assert!(first_value.starts_with("row_0_"));

    println!("Decompression verified for {} rows", total_rows);
}
```

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

## 4.7 Connection - get_table_schema() [COMPLETED]

### Objective
Implement efficient single-table schema retrieval.

### Implementation Summary (Completed 2024-12-16)

The implementation executes a DESCRIBE TABLE query via SEA API and parses the result into an Arrow Schema.

**Key Components:**
- `get_table_schema()`: Main method that constructs and executes DESCRIBE TABLE query
- `describe_result_to_schema()`: Parses SEA response into Arrow Schema
- `spark_type_to_arrow()`: Maps Spark SQL type strings to Arrow DataType
- Helper functions for parsing complex types (decimal, array, map, struct)

**Type Mapping Support:**
- Simple types: boolean, int, bigint, string, binary, date, timestamp, etc.
- Parameterized types: decimal(p,s), varchar(n), char(n)
- Complex types: array<T>, map<K,V>, struct<field:type,...>

**Identifier Handling:**
- Uses backtick quoting for safe identifier handling
- Falls back to connection's current_catalog/current_schema when not specified

### Actual Implementation

```rust
impl Connection for DatabricksConnection {
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        // Use provided values or fall back to connection defaults
        let catalog_name = catalog
            .or(self.current_catalog.as_deref())
            .unwrap_or("main");
        let schema_name = db_schema
            .or(self.current_schema.as_deref())
            .unwrap_or("default");

        // Construct with backtick quoting for safety
        let sql = format!(
            "DESCRIBE TABLE `{}`.`{}`.`{}`",
            catalog_name, schema_name, table_name
        );

        // Execute via SEA client directly
        let response = block_on_async(&self.runtime, async move {
            client.execute_and_wait(&session_id, &sql, None, None, None).await
        })?;

        // Parse DESCRIBE result into Arrow Schema
        self.describe_result_to_schema(&response)
    }
}
```

### Test Types
- **Unit Tests**: Schema parsing from DESCRIBE output, type mapping tests
- **E2E Tests**: Retrieve schema for real tables from Databricks

### E2E Tests Implemented

1. `test_e2e_get_table_schema_system_table` - Retrieves schema for system.information_schema.tables
2. `test_e2e_get_table_schema_configured_table` - Retrieves schema for configured test table
3. `test_e2e_get_table_schema_uses_connection_defaults` - Verifies catalog/schema defaults work
4. `test_e2e_get_table_schema_nonexistent_table` - Verifies error handling for invalid tables
5. `test_e2e_get_table_schema_type_mapping` - Verifies type mapping with information_schema.columns

### E2E Exit Criteria
✅ **COMPLETED**: All E2E tests pass against real Databricks instance

```rust
#[test]
#[ignore]
fn test_e2e_get_table_schema_system_table() {
    // Retrieves schema for system.information_schema.tables
    // Verifies expected columns: table_catalog, table_schema, table_name, table_type
}

#[test]
#[ignore]
fn test_e2e_get_table_schema_configured_table() {
    // Uses config.metadata.catalog/schema/table from test config
    // Verifies column count matches config.metadata.expected_column_count
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

### Test Types
- **Unit Tests**: Affected row parsing from API responses
- **Integration Tests**: execute_update with mocked responses
- **E2E Tests**: INSERT, UPDATE, DELETE, and DDL operations with real Databricks

### Expected Results

| Result | Verification | Test Type |
|--------|--------------|-----------|
| Returns affected rows | UPDATE/DELETE returns count | E2E |
| Non-SELECT queries work | CREATE TABLE succeeds | E2E |
| Returns -1 for DDL | Schema modifications don't report rows | E2E |
| Respects statement.rows_affected | If user set rows_affected, use that | Unit |

### E2E Exit Criteria
✅ **E2E Test**: `test_e2e_execute_update_dml` - Execute INSERT, UPDATE, DELETE with real Databricks

```rust
#[test]
#[ignore]
fn test_e2e_execute_update_dml() {
    skip_if_no_config!();

    let config = get_test_config();
    let mut conn = create_test_connection();

    // Create temp table
    let temp_table = format!("{}.{}.test_update_temp", config.metadata.catalog, config.metadata.schema);

    let mut stmt = conn.new_statement().unwrap();

    // CREATE TABLE
    stmt.set_sql_query(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT, name STRING)",
        temp_table
    )).unwrap();
    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, -1, "DDL should return -1");

    // INSERT
    stmt.set_sql_query(&format!(
        "INSERT INTO {} VALUES (1, 'Alice'), (2, 'Bob')",
        temp_table
    )).unwrap();
    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, 2);

    // UPDATE
    stmt.set_sql_query(&format!(
        "UPDATE {} SET name = 'Charlie' WHERE id = 1",
        temp_table
    )).unwrap();
    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, 1);

    // DELETE
    stmt.set_sql_query(&format!(
        "DELETE FROM {} WHERE id = 2",
        temp_table
    )).unwrap();
    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, 1);

    // Cleanup
    stmt.set_sql_query(&format!("DROP TABLE {}", temp_table)).unwrap();
    stmt.execute_update().unwrap();

    println!("DML operations verified");
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

## 5.1 Unit Test Suite

### Objective
Comprehensive unit tests for all components using mocking where appropriate.

### Actions

1. **Error Mapping Tests** (`tests/unit/error_tests.rs`)
   - All SEA error codes → ADBC status
   - Retryable error detection
   - Error message preservation

2. **Retry Logic Tests** (`tests/unit/retry_tests.rs`)
   - Exponential backoff timing
   - Jitter range validation
   - Max retries enforcement
   - Retry-After header handling

3. **Type Conversion Tests** (`tests/unit/type_mapping_tests.rs`)
   - All Spark SQL types → Arrow types
   - DECIMAL precision/scale
   - Complex types (ARRAY, MAP, STRUCT)
   - Edge cases (NULL, empty strings)

4. **Mock HTTP Tests** (`tests/unit/client_tests.rs`)
   - Use wiremock to simulate SEA API responses
   - Test polling behavior
   - Test error response parsing
   - Test URL construction

### Expected Results
- All unit tests pass
- Code coverage > 80% for core modules
- Fast execution (< 5 seconds total)

---

## 5.2 Integration Test Suite

### Objective
Integration tests against local mock server or lightweight test environment.

### Actions

1. **Connection Lifecycle** (`tests/integration/connection_tests.rs`)
   - Session creation and termination
   - Connection options
   - Multiple connections from same database

2. **Query Execution** (`tests/integration/query_tests.rs`)
   - Simple SELECT queries
   - Large result sets (mocked external links)
   - Empty result sets
   - Query cancellation

3. **Metadata APIs** (`tests/integration/metadata_tests.rs`)
   - get_info
   - get_objects (all depths)
   - get_table_schema
   - get_table_types

### Expected Results
- All integration tests pass
- Can run without real Databricks connection
- Reasonable execution time (< 30 seconds)

---

## 5.3 E2E Test Suite Completion & Verification

### Objective
Verify all E2E tests from earlier sprints pass and add any remaining comprehensive end-to-end scenarios.

### Note
**E2E test infrastructure was established in Sprint 1 (work item 1.2a)**, and E2E tests have been integrated into Sprints 1-4 as exit criteria for each work item. This section focuses on ensuring all E2E tests pass together and adding comprehensive workflow scenarios.

### Actions

1. **Run all E2E tests from earlier sprints and verify they pass**
   ```bash
   cargo test --ignored --package adbc-driver-databricks -- --test-threads=1
   ```

2. **E2E Tests from Sprint 1:**
   - `test_e2e_config_and_connect` - Configuration loading and basic connection (1.2a)
   - `test_e2e_session_create_and_terminate` - Session lifecycle (1.4)

3. **E2E Tests from Sprint 2:**
   - `test_e2e_connection_lifecycle` - Connection creation and cleanup
   - `test_e2e_query_select_basic` - Basic SELECT queries
   - `test_e2e_query_data_types` - Data type handling

5. **E2E Tests from Sprint 3:**
   - `test_e2e_query_large_result` - Large result sets with external links
   - `test_e2e_query_external_links_decompression` - LZ4 decompression

6. **E2E Tests from Sprint 4:**
   - `test_e2e_metadata_get_table_schema` - Metadata retrieval
   - `test_e2e_execute_update_dml` - DML operations

7. **Add comprehensive workflow E2E tests**

   Create `tests/e2e/workflows.rs` for end-to-end workflow scenarios:

   ```rust
   // Test complete workflow from connection to query execution
   #[test]
   #[ignore]
   fn test_e2e_complete_workflow() {
       skip_if_no_config!();

       // 1. Create connection
       let mut conn = create_test_connection();

       // 2. Execute query
       let mut stmt = conn.new_statement().unwrap();
       stmt.set_sql_query("SELECT 1 AS one").unwrap();
       let mut reader = stmt.execute().unwrap();

       // 3. Read results
       let batch = reader.next().unwrap().unwrap();
       assert_eq!(batch.num_rows(), 1);

       // 4. Verify data
       let array = batch.column(0).as_any()
           .downcast_ref::<Int32Array>().unwrap();
       assert_eq!(array.value(0), 1);

       // Connection auto-closed on drop
   }

   // Test concurrent queries on same connection
   #[test]
   #[ignore]
   fn test_e2e_concurrent_statements() {
       skip_if_no_config!();

       let mut conn = create_test_connection();

       // Execute multiple queries sequentially
       for i in 1..=5 {
           let mut stmt = conn.new_statement().unwrap();
           stmt.set_sql_query(&format!("SELECT {} AS num", i)).unwrap();
           let mut reader = stmt.execute().unwrap();
           let batch = reader.next().unwrap().unwrap();
           let array = batch.column(0).as_any()
               .downcast_ref::<Int32Array>().unwrap();
           assert_eq!(array.value(0), i);
       }
   }
   ```

### Test Types
- **E2E Tests**: All integrated tests from Sprints 0-4, plus comprehensive workflows

### Expected Results

| Result | Verification | Test Type |
|--------|--------------|-----------|
| All Sprint 1-4 E2E tests pass | Test suite succeeds | E2E |
| Workflow scenarios work | Complete workflows execute | E2E |
| No regressions | Previously passing tests still pass | E2E |

### E2E Exit Criteria
✅ **All E2E tests pass**: Run full E2E suite with real Databricks and verify 100% pass rate

---

## 5.4 Documentation & Examples

### Objective
Complete driver documentation, usage examples, and README.

### Actions

1. **Main README.md** (`driver/databricks/README.md`)
   - Installation instructions
   - Quick start example
   - Configuration options
   - Feature list
   - Link to examples

2. **API Documentation** (Rust doc comments)
   - Document all public APIs
   - Add usage examples to key functions
   - Document error scenarios

3. **Examples directory** (`driver/databricks/examples/`)
   - `basic_query.rs` - Simple SELECT example
   - `large_result.rs` - Handling large result sets
   - `metadata.rs` - Using metadata APIs
   - `error_handling.rs` - Proper error handling patterns

4. **Configuration guide** (`driver/databricks/docs/configuration.md`)
   - All connection options
   - Authentication methods
   - Performance tuning
   - Troubleshooting

---

## 5.5 CI/CD Integration for E2E Tests

### Objective
Set up automated E2E test execution in CI pipeline with proper configuration management.

### Actions

1. **GitHub Actions workflow** (`.github/workflows/e2e-tests.yml`)
   - Trigger on PR to main branch
   - Use GitHub Secrets for Databricks credentials
   - Store test configuration securely
   - Run E2E tests against dedicated test warehouse

2. **Test configuration management**
   - Store encrypted test credentials as GitHub Secrets
   - Generate JSON configuration file in CI
   - Set `DATABRICKS_TEST_CONFIG_FILE` environment variable

3. **Test reporting**
   - Collect test results
   - Generate coverage reports
   - Archive test artifacts

4. **Conditional execution**
   - Only run when Rust code changes
   - Allow manual trigger for releases

---

## 5.6 Performance Optimization & Benchmarking

### Objective
Optimize performance and add benchmarks.

### Actions

1. **Add benchmarks** (`benches/`)
   - Query execution speed
   - Arrow conversion overhead
   - External links streaming performance

2. **Profile and optimize**
   - Identify bottlenecks with profiling tools
   - Optimize hot paths
   - Reduce allocations where possible

3. **Memory usage testing**
   - Verify constant memory usage for streaming
   - Test with very large result sets
   - Check for memory leaks

---

## 5.7 Release Preparation

### Objective
Prepare for initial release.

### Actions

1. **Version management**
   - Set initial version (0.1.0)
   - Document version strategy
   - Create CHANGELOG.md

2. **License and legal**
   - Verify all dependencies are compatible
   - Add LICENSE file
   - Add copyright headers

3. **Release checklist**
   - All tests passing (unit, integration, E2E)
   - Documentation complete
   - Examples working
   - README.md polished
   - CHANGELOG.md updated

4. **Crates.io preparation** (if publishing)
   - Verify Cargo.toml metadata
   - Add keywords and categories
   - Test `cargo publish --dry-run`

---

# Sprint Summary

## Sprint 1: Foundation (Duration: 7 days)
- Project structure and dependencies
- Error types and ADBC status mapping
- E2E test configuration and helpers (1.2a)
- Test data setup in Databricks
- HTTP client and session management
- **E2E: Config/connection smoke test, Session lifecycle test**

## Sprint 2: Basic Execution (Duration: 7 days)
- DatabricksConnection implementation
- DatabricksStatement basics
- Inline query execution
- Schema parsing
- **E2E: Basic queries, data types**

## Sprint 3: External Links (Duration: 5 days)
- Worker pool-based chunk fetching
- LZ4 decompression
- Ordered queue processing with fixed concurrency
- Statement cancellation
- **E2E: Large results, external links**

## Sprint 4: Metadata & Updates (Duration: 5 days)
- get_info, get_objects implementations
- get_table_schema
- execute_update for DML/DDL
- **E2E: Metadata APIs, DML operations**

## Sprint 5: Polish (Duration: 6 days)
- Complete test suites (unit, integration)
- **E2E: Verify all tests pass together**
- Documentation and examples
- CI/CD setup
- Performance optimization
- Release preparation

**Total Duration: ~30 days (6 weeks)**

---

# Testing Strategy Summary

## Test Pyramid

```
         /\
        /E2\      E2E Tests (Real Databricks)
       /2E2E\     - Continuous from Sprint 1 (1.2a)
      /______\    - Each work item has E2E exit criteria
     /        \
    /Integration\  Integration Tests
   /____________\  - Mock HTTP responses
  /              \ - Component interactions
 /   Unit Tests   \
/__________________\
```

## E2E Test Distribution

| Sprint | E2E Tests Added |
|--------|----------------|
| Sprint 1 | Config & connection smoke test (1.2a), Session lifecycle (1.4) |
| Sprint 2 | Basic queries, data types, connection lifecycle |
| Sprint 3 | Large results, external links, decompression |
| Sprint 4 | Metadata APIs, DML operations |
| Sprint 5 | Complete workflows, verify all tests pass |

## Key Principles

1. **Early E2E Testing**: E2E tests start in Sprint 1 (1.2a), not Sprint 5
2. **Continuous Validation**: Each work item validates against real Databricks
3. **Clear Exit Criteria**: Every applicable work item has E2E test requirement
4. **Test Type Clarity**: Each section specifies Unit/Integration/E2E tests
5. **Real-world Focus**: E2E tests use actual Databricks SQL Warehouse

---

# Appendix: E2E Test Quick Reference

## Running E2E Tests

```bash
# Set up configuration
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks.json

# Run all E2E tests
cargo test --ignored --package adbc-driver-databricks

# Run specific E2E test
cargo test --ignored test_e2e_query_select_basic

# Run with output
cargo test --ignored -- --nocapture
```

## Configuration File Example

**Format:** Matches C# ADBC driver configuration for cross-language compatibility

```json
{
  "environment": "Databricks",
  "uri": "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456",
  "token": "dapi...",
  "query": "select count(*) from `main`.`my_schema`.`my_table`",
  "type": "databricks",
  "trace": "true",
  "expectedResults": 1,
  "metadata": {
    "catalog": "main",
    "schema": "my_schema",
    "table": "my_table",
    "expectedColumnCount": 3
  }
}
```

**Field Descriptions:**
- `environment`: Environment name (always "Databricks")
- `uri`: Full URI with host and warehouse ID combined
- `token`: Personal Access Token for authentication
- `query`: Optional test query to validate configuration
- `type`: Driver type (always "databricks")
- `trace`: Enable trace logging ("true" or "false")
- `expectedResults`: Expected number of results from test query
- `metadata.catalog`: Catalog name for test operations
- `metadata.schema`: Schema name for test operations
- `metadata.table`: Table name for test operations
- `metadata.expectedColumnCount`: Expected column count for metadata validation

## E2E Test Checklist

- [ ] Sprint 1 (1.2a): test_e2e_config_and_connect
- [ ] Sprint 1 (1.4): test_e2e_session_create_and_terminate
- [ ] Sprint 2: test_e2e_connection_lifecycle
- [ ] Sprint 2: test_e2e_query_select_basic
- [ ] Sprint 2: test_e2e_query_data_types
- [ ] Sprint 3: test_e2e_query_large_result
- [ ] Sprint 3: test_e2e_query_external_links_decompression
- [x] Sprint 4: test_e2e_metadata_get_table_schema (Completed as test_e2e_get_table_schema_*)
- [ ] Sprint 4: test_e2e_execute_update_dml
- [ ] Sprint 5: test_e2e_complete_workflow
- [ ] Sprint 5: test_e2e_concurrent_statements

---

**End of Implementation Plan**
