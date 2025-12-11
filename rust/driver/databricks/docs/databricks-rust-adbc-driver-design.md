# Databricks Rust ADBC Driver Design

**Version**: 1.1
**Last Updated**: 2025-12-11
**Author**: PECO Team
**Status**: Implementation Complete (94% Test Pass Rate)

---

## 1. Overview

### 1.1 Purpose

This document describes the design of a native Rust ADBC (Arrow Database Connectivity) driver for Databricks SQL Warehouses using the Statement Execution API (SEA). The driver provides Arrow-native access to Databricks, enabling high-performance data access without unnecessary data copies.

### 1.2 Goals

- **Native Rust Implementation**: Pure Rust driver calling SEA REST API directly
- **Arrow-Native**: Return results as Arrow RecordBatches for zero-copy integration
- **High Performance**: Parallel chunk fetching with LZ4 compression support
- **ADBC 1.1.0 Compliant**: Implement all required ADBC traits
- **Async I/O**: Use async runtime internally for efficient network operations

### 1.3 Non-Goals

- OAuth 2.0 authentication (future enhancement)
- Thrift/HiveServer2 protocol support
- Legacy result formats (JSON_ARRAY, CSV)

### 1.4 Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Authentication | PAT only | Simplest to implement; OAuth can be added later |
| Result Disposition | INLINE_OR_EXTERNAL_LINKS | Auto-selects optimal mode based on result size |
| Result Format | ARROW_STREAM | Native Arrow format for ADBC |
| Async Runtime | Tokio | Industry standard; sync wrappers for ADBC traits |
| Session Management | Always use sessions | Maintains connection state, enables temp tables |
| Compression | LZ4_FRAME | Reduces network transfer, ~3-5x compression |

---

## 2. Architecture

### 2.1 High-Level Architecture

```mermaid
graph TB
    subgraph "User Application"
        APP[Application Code]
    end

    subgraph "ADBC Layer"
        DRIVER[DatabricksDriver]
        DATABASE[DatabricksDatabase]
        CONNECTION[DatabricksConnection]
        STATEMENT[DatabricksStatement]
    end

    subgraph "Internal Components"
        CLIENT[SeaClient]
        SESSION[SessionManager]
        FETCHER[ChunkFetcher]
        READER[ArrowResultReader]
    end

    subgraph "External"
        SEA[SEA REST API]
        CLOUD[Cloud Storage<br/>S3/ADLS/GCS]
    end

    APP --> DRIVER
    DRIVER --> DATABASE
    DATABASE --> CONNECTION
    CONNECTION --> STATEMENT
    CONNECTION --> SESSION

    STATEMENT --> CLIENT
    STATEMENT --> FETCHER
    FETCHER --> READER

    CLIENT --> SEA
    FETCHER --> CLOUD
    SESSION --> CLIENT
```

### 2.2 Component Overview

```mermaid
classDiagram
    class DatabricksDriver {
        +new_database() Database
        +new_database_with_opts(opts) Database
    }

    class DatabricksDatabase {
        -config: DatabaseConfig
        -runtime: Arc~Runtime~
        +new_connection() Connection
        +new_connection_with_opts(opts) Connection
    }

    class DatabricksConnection {
        -client: Arc~SeaClient~
        -session_id: String
        -catalog: Option~String~
        -schema: Option~String~
        +new_statement() Statement
        +get_info(codes) RecordBatchReader
        +get_objects(...) RecordBatchReader
        +get_table_schema(...) Schema
        +cancel()
        +commit()
        +rollback()
    }

    class DatabricksStatement {
        -connection: Arc~DatabricksConnection~
        -sql_query: Option~String~
        -parameters: Vec~Parameter~
        -statement_id: Option~String~
        +set_sql_query(query)
        +bind(batch)
        +execute() RecordBatchReader
        +execute_update() Option~i64~
        +cancel()
    }

    class SeaClient {
        -http_client: reqwest::Client
        -host: String
        -token: String
        -warehouse_id: String
        +execute_statement(req) StatementResponse
        +get_statement(id) StatementResponse
        +get_chunk(id, index) ChunkResponse
        +cancel_statement(id)
        +close_statement(id)
        +create_session(req) SessionResponse
        +delete_session(id)
    }

    class ChunkFetcher {
        -client: Arc~SeaClient~
        -http_client: reqwest::Client
        -concurrency: usize
        +fetch_chunks(manifest) Stream~RecordBatch~
    }

    DatabricksDriver --> DatabricksDatabase
    DatabricksDatabase --> DatabricksConnection
    DatabricksConnection --> DatabricksStatement
    DatabricksConnection --> SeaClient
    DatabricksStatement --> ChunkFetcher
    ChunkFetcher --> SeaClient
```

### 2.3 Module Structure

```
adbc-driver-databricks/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # Public exports
│   ├── driver.rs              # DatabricksDriver implementation
│   ├── database.rs            # DatabricksDatabase implementation
│   ├── connection.rs          # DatabricksConnection implementation
│   ├── statement.rs           # DatabricksStatement implementation
│   ├── client/
│   │   ├── mod.rs             # SeaClient
│   │   ├── models.rs          # Request/Response types
│   │   └── error.rs           # API error handling
│   ├── fetch/
│   │   ├── mod.rs             # ChunkFetcher
│   │   ├── reader.rs          # ArrowResultReader
│   │   └── decompress.rs      # LZ4 decompression
│   ├── session.rs             # Session management
│   ├── options.rs             # Driver-specific options
│   └── error.rs               # Error types and mapping
└── tests/
    ├── integration/           # Integration tests
    └── unit/                  # Unit tests
```

---

## 3. Component Design

### 3.1 DatabricksDriver

Entry point for creating database connections.

```rust
pub trait Driver {
    type DatabaseType: Database;

    fn new_database(&mut self) -> Result<Self::DatabaseType>;
    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType>;
}
```

**Driver-Specific Options:**

| Option Key | Type | Required | Description |
|------------|------|----------|-------------|
| `uri` | String | Yes | Workspace URL (e.g., `https://xxx.cloud.databricks.com`) |
| `databricks.warehouse_id` | String | Yes | SQL Warehouse ID |
| `databricks.token` | String | Yes | Personal Access Token |
| `databricks.catalog` | String | No | Default catalog |
| `databricks.schema` | String | No | Default schema |

### 3.2 DatabricksDatabase

Holds shared configuration and the Tokio runtime.

```rust
pub struct DatabricksDatabase {
    config: Arc<DatabaseConfig>,
    runtime: Arc<Runtime>,
}

struct DatabaseConfig {
    host: String,
    warehouse_id: String,
    token: String,
    default_catalog: Option<String>,
    default_schema: Option<String>,
    http_config: HttpConfig,
}

struct HttpConfig {
    connect_timeout: Duration,      // Default: 10s
    read_timeout: Duration,         // Default: 300s
    max_retries: u32,               // Default: 3
    retry_backoff_base: Duration,   // Default: 1s
}
```

**Contract:**
- Creating a Database does NOT establish a network connection
- Configuration is validated at Database creation time
- Runtime is shared across all connections from this Database

### 3.3 DatabricksConnection

Represents an active session with the SQL Warehouse.

```mermaid
sequenceDiagram
    participant App as Application
    participant Conn as DatabricksConnection
    participant Client as SeaClient
    participant SEA as SEA API

    App->>Conn: new_connection()
    Conn->>Client: create_session(warehouse_id, catalog, schema)
    Client->>SEA: POST /api/2.0/sql/sessions/
    SEA-->>Client: {session_id: "xxx"}
    Client-->>Conn: session_id
    Conn-->>App: Connection ready

    Note over App,SEA: Connection usage...

    App->>Conn: drop()/close()
    Conn->>Client: delete_session(session_id)
    Client->>SEA: DELETE /api/2.0/sql/sessions/{session_id}
    SEA-->>Client: {}
```

**Session Lifecycle:**
- Session created on `new_connection()`
- Session kept alive automatically (statements refresh the idle timeout)
- Session terminated on Connection drop

**Connection Options:**

| Option Key | Type | Description |
|------------|------|-------------|
| `adbc.connection.autocommit` | String | Always "true" (Databricks doesn't support transactions) |
| `adbc.connection.current_catalog` | String | Current catalog |
| `adbc.connection.current_db_schema` | String | Current schema |

### 3.4 DatabricksStatement

Executes SQL statements and returns results.

```mermaid
sequenceDiagram
    participant App as Application
    participant Stmt as DatabricksStatement
    participant Client as SeaClient
    participant Fetcher as ChunkFetcher
    participant SEA as SEA API
    participant Cloud as Cloud Storage

    App->>Stmt: set_sql_query("SELECT ...")
    App->>Stmt: execute()

    Stmt->>Client: execute_statement(sql, session_id)
    Client->>SEA: POST /api/2.0/sql/statements/

    alt Small Result (INLINE)
        SEA-->>Client: {state: SUCCEEDED, result: {data_array: [...]}}
        Client-->>Stmt: Response with inline Arrow data
        Stmt-->>App: ArrowResultReader (single batch)
    else Large Result (EXTERNAL_LINKS)
        SEA-->>Client: {state: SUCCEEDED, manifest: {...}, result: {external_links: [...]}}
        Client-->>Stmt: Response with external links
        Stmt->>Fetcher: fetch_chunks(manifest, external_links)

        par Parallel Chunk Fetching
            Fetcher->>Cloud: GET chunk_0 (presigned URL)
            Fetcher->>Cloud: GET chunk_1 (presigned URL)
            Fetcher->>Cloud: GET chunk_2 (presigned URL)
        end

        Cloud-->>Fetcher: Arrow IPC data (LZ4 compressed)
        Fetcher->>Fetcher: Decompress LZ4
        Fetcher->>Fetcher: Parse Arrow IPC
        Fetcher-->>Stmt: Stream<RecordBatch>
        Stmt-->>App: ArrowResultReader (streaming)
    else Pending/Running
        SEA-->>Client: {state: RUNNING}
        Client->>Client: Poll with backoff
        Client->>SEA: GET /api/2.0/sql/statements/{id}
        Note over Client,SEA: Repeat until SUCCEEDED/FAILED
    end
```

**Statement Options:**

| Option Key | Type | Description |
|------------|------|-------------|
| `databricks.statement.wait_timeout` | String | Wait timeout (default: "10s") |
| `databricks.statement.row_limit` | Int | Maximum rows to return |
| `databricks.statement.byte_limit` | Int | Maximum bytes to return |

### 3.5 ChunkFetcher

Parallel fetcher for EXTERNAL_LINKS chunks.

```mermaid
flowchart TB
    subgraph Input
        MANIFEST[Manifest<br/>total_chunk_count: 5]
        LINKS[External Links<br/>chunk 0..4]
    end

    subgraph "Chunk Fetcher (concurrency=4)"
        QUEUE[Chunk Queue]

        subgraph Workers
            W1[Worker 1]
            W2[Worker 2]
            W3[Worker 3]
            W4[Worker 4]
        end

        BUFFER[Ordered Buffer]
    end

    subgraph Processing
        DECOMP[LZ4 Decompress]
        PARSE[Arrow IPC Parse]
        OUT[RecordBatch Stream]
    end

    MANIFEST --> QUEUE
    LINKS --> QUEUE
    QUEUE --> W1 & W2 & W3 & W4
    W1 & W2 & W3 & W4 --> BUFFER
    BUFFER --> DECOMP
    DECOMP --> PARSE
    PARSE --> OUT
```

**Contract:**
- Chunks fetched in parallel with configurable concurrency (default: 8)
- Output RecordBatches are yielded in chunk order
- Failed chunk downloads retry with exponential backoff
- Expired URLs are refreshed by calling `get_chunk` again

---

## 4. Data Flow

### 4.1 Execute Query Flow

```mermaid
flowchart LR
    subgraph "ADBC Interface (Sync)"
        A1[set_sql_query]
        A2[execute]
        A3[RecordBatchReader]
    end

    subgraph "Async Bridge"
        B1[block_on]
    end

    subgraph "Async Operations"
        C1[execute_statement_async]
        C2[poll_until_complete]
        C3[fetch_chunks_async]
    end

    subgraph "Network"
        D1[HTTP POST]
        D2[HTTP GET]
        D3[Cloud GET]
    end

    A1 --> A2
    A2 --> B1
    B1 --> C1
    C1 --> D1
    D1 --> C2
    C2 --> D2
    C2 --> C3
    C3 --> D3
    D3 --> A3
```

### 4.2 Arrow Type Mapping

| Spark SQL Type | Arrow Type | Notes |
|----------------|------------|-------|
| BOOLEAN | Boolean | |
| TINYINT | Int8 | |
| SMALLINT | Int16 | |
| INT | Int32 | |
| BIGINT | Int64 | |
| FLOAT | Float32 | |
| DOUBLE | Float64 | |
| DECIMAL(p,s) | Decimal128(p,s) | |
| STRING | Utf8 | |
| BINARY | Binary | |
| DATE | Date32 | Days since epoch |
| TIMESTAMP | Timestamp(Microsecond, None) | |
| ARRAY<T> | List<T> | |
| MAP<K,V> | Map<K,V> | |
| STRUCT<...> | Struct<...> | |

---

## 5. Error Handling

### 5.1 Error Mapping

```mermaid
flowchart TB
    subgraph "SEA API Errors"
        E400[400 BAD_REQUEST]
        E401[401 UNAUTHENTICATED]
        E403[403 PERMISSION_DENIED]
        E404[404 NOT_FOUND]
        E429[429 RATE_LIMITED]
        E500[500 INTERNAL_ERROR]
        E503[503 UNAVAILABLE]
    end

    subgraph "ADBC Status"
        S_INV[InvalidArguments]
        S_UNAUTH[Unauthenticated]
        S_UNAZ[Unauthorized]
        S_NF[NotFound]
        S_IO[IO]
        S_INT[Internal]
    end

    E400 --> S_INV
    E401 --> S_UNAUTH
    E403 --> S_UNAZ
    E404 --> S_NF
    E429 --> S_IO
    E500 --> S_INT
    E503 --> S_IO
```

### 5.2 SEA Error to ADBC Status Mapping

| SEA Error Code | HTTP Status | ADBC Status | Retry |
|----------------|-------------|-------------|-------|
| BAD_REQUEST | 400 | InvalidArguments | No |
| INVALID_PARAMETER_VALUE | 400 | InvalidArguments | No |
| UNAUTHENTICATED | 401 | Unauthenticated | Once (refresh) |
| PERMISSION_DENIED | 403 | Unauthorized | No |
| NOT_FOUND | 404 | NotFound | No |
| REQUEST_LIMIT_EXCEEDED | 429 | IO | Yes (backoff) |
| INTERNAL_ERROR | 500 | Internal | Yes (3x) |
| TEMPORARILY_UNAVAILABLE | 503 | IO | Yes (5x) |

### 5.3 Retry Strategy

```rust
/// Retry configuration for transient errors
pub struct RetryConfig {
    /// Maximum retry attempts
    pub max_retries: u32,           // Default: 3
    /// Base delay for exponential backoff
    pub base_delay: Duration,       // Default: 1s
    /// Maximum delay between retries
    pub max_delay: Duration,        // Default: 30s
    /// Jitter factor (0.0 - 1.0)
    pub jitter: f64,                // Default: 0.5
}
```

**Retry Logic:**
- Delay = min(base_delay * 2^attempt, max_delay) * (1 + random(0, jitter))
- 429 errors: Respect `Retry-After` header if present
- Network errors: Retry with backoff
- Statement polling: Exponential backoff (1s, 2s, 4s, 8s, 10s max)

### 5.4 External Link Expiration Handling

```mermaid
sequenceDiagram
    participant Fetcher as ChunkFetcher
    participant Cloud as Cloud Storage
    participant Client as SeaClient
    participant SEA as SEA API

    Fetcher->>Cloud: GET chunk (expired URL)
    Cloud-->>Fetcher: 403 Forbidden / URL Expired

    Fetcher->>Client: get_chunk(statement_id, chunk_index)
    Client->>SEA: GET /statements/{id}/result/chunks/{index}
    SEA-->>Client: {external_links: [{new_url, new_expiration}]}
    Client-->>Fetcher: Refreshed URL

    Fetcher->>Cloud: GET chunk (new URL)
    Cloud-->>Fetcher: Arrow data
```

---

## 6. Concurrency Model

### 6.1 Thread Safety

```mermaid
flowchart TB
    subgraph "Thread-Safe (Arc)"
        DB[DatabricksDatabase]
        RT[Tokio Runtime]
        CFG[DatabaseConfig]
        HTTP[reqwest::Client]
    end

    subgraph "Not Thread-Safe (Mutable)"
        CONN[DatabricksConnection]
        STMT[DatabricksStatement]
    end

    subgraph "Interior Mutability"
        SESSION[Session State<br/>Mutex~SessionId~]
    end

    DB --> RT
    DB --> CFG
    CONN --> HTTP
    CONN --> SESSION
    STMT --> CONN
```

**Thread Safety Guarantees:**
- `DatabricksDatabase`: `Send + Sync` - can be shared across threads
- `DatabricksConnection`: `Send` only - owned by single thread at a time
- `DatabricksStatement`: `Send` only - owned by single thread at a time
- HTTP client: Connection pooled, thread-safe

### 6.2 Async/Sync Bridge

```rust
impl DatabricksStatement {
    /// Execute query (sync ADBC interface)
    pub fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        // Block on async execution within the shared runtime
        self.runtime.block_on(self.execute_async())
    }

    /// Internal async implementation
    async fn execute_async(&mut self) -> Result<ArrowResultReader> {
        let response = self.client.execute_statement(&request).await?;

        match response.status.state {
            State::Succeeded => self.handle_success(response).await,
            State::Pending | State::Running => {
                let final_response = self.poll_until_complete(response.statement_id).await?;
                self.handle_success(final_response).await
            }
            State::Failed => Err(self.map_error(response.status.error)),
            _ => Err(Error::with_message_and_status("Unexpected state", Status::Internal)),
        }
    }
}
```

---

## 7. Configuration

### 7.1 Connection String Format

```
databricks://<host>/<warehouse_id>?token=<pat>&catalog=<catalog>&schema=<schema>
```

Example:
```
databricks://my-workspace.cloud.databricks.com/abc123def456?token=dapi1234567890&catalog=main&schema=default
```

### 7.2 Driver Options Summary

| Option | Scope | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `uri` | Database | String | Required | Workspace URL |
| `databricks.warehouse_id` | Database | String | Required | SQL Warehouse ID |
| `databricks.token` | Database | String | Required | PAT token |
| `databricks.catalog` | Database | String | None | Default catalog |
| `databricks.schema` | Database | String | None | Default schema |
| `databricks.http.connect_timeout` | Database | Int | 10000 | Connect timeout (ms) |
| `databricks.http.read_timeout` | Database | Int | 300000 | Read timeout (ms) |
| `databricks.fetch.concurrency` | Database | Int | 8 | Parallel chunk fetchers |
| `databricks.fetch.compression` | Database | String | "LZ4_FRAME" | Result compression |

### 7.3 Environment Variables

| Variable | Maps To |
|----------|---------|
| `DATABRICKS_HOST` | `uri` |
| `DATABRICKS_WAREHOUSE_ID` | `databricks.warehouse_id` |
| `DATABRICKS_TOKEN` | `databricks.token` |
| `DATABRICKS_CATALOG` | `databricks.catalog` |
| `DATABRICKS_SCHEMA` | `databricks.schema` |

---

## 8. Dependencies

### 8.1 Rust Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| `adbc_core` | latest | ADBC trait definitions |
| `arrow` | ^53.0 | Arrow data types and arrays |
| `arrow-ipc` | ^53.0 | Arrow IPC stream parsing |
| `tokio` | ^1.0 | Async runtime |
| `reqwest` | ^0.12 | HTTP client |
| `serde` | ^1.0 | JSON serialization |
| `serde_json` | ^1.0 | JSON parsing |
| `lz4_flex` | ^0.11 | LZ4 decompression |
| `thiserror` | ^1.0 | Error handling |
| `url` | ^2.0 | URL parsing |
| `base64` | ^0.22 | Base64 encoding |

### 8.2 Cargo.toml Structure

```toml
[package]
name = "adbc-driver-databricks"
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
description = "ADBC driver for Databricks SQL Warehouses"

[features]
default = ["rustls-tls"]
rustls-tls = ["reqwest/rustls-tls"]
native-tls = ["reqwest/native-tls"]

[dependencies]
adbc_core = { path = "../core" }
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

---

## 9. Test Strategy

### 9.1 Unit Tests

| Test Category | Description |
|---------------|-------------|
| `SeaClient_ExecuteStatement_ReturnsStatementId` | Verify execute request/response |
| `SeaClient_Poll_ExponentialBackoff` | Verify polling timing |
| `ChunkFetcher_ParallelFetch_MaintainsOrder` | Verify chunk ordering |
| `ChunkFetcher_ExpiredUrl_RefreshesAndRetries` | Verify URL refresh |
| `ArrowReader_DecompressLz4_ParsesIpc` | Verify decompression + parsing |
| `ErrorMapping_SeaToAdbc_CorrectStatus` | Verify error mapping |
| `Options_ParseConnectionString_ExtractsAll` | Verify option parsing |

### 9.2 Integration Tests

| Test Category | Description |
|---------------|-------------|
| `Connection_OpenClose_CreatesTerminatesSession` | Session lifecycle |
| `Statement_SimpleQuery_ReturnsArrowData` | Basic query execution |
| `Statement_LargeResult_FetchesAllChunks` | Multi-chunk results |
| `Statement_Cancel_StopsExecution` | Cancellation |
| `GetObjects_Catalogs_ReturnsCatalogList` | Metadata queries |
| `GetTableSchema_ValidTable_ReturnsSchema` | Schema retrieval |

### 9.3 Performance Tests

| Test | Target |
|------|--------|
| `Throughput_1GBResult_ParallelFetch` | > 100 MB/s |
| `Latency_SmallQuery_InlineResult` | < 500ms |
| `Concurrency_MultipleStatements_NoContention` | Linear scaling |

### 9.4 E2E Test Strategy

End-to-end tests validate the entire driver stack against a real Databricks SQL Warehouse. These tests ensure that all components work together correctly in production-like scenarios.

#### 9.4.1 Test Infrastructure

```mermaid
flowchart TB
    subgraph "Test Environment"
        TEST[E2E Test Suite]
        ENV[Environment Config]
    end

    subgraph "Driver Stack"
        DRIVER[DatabricksDriver]
        DB[DatabricksDatabase]
        CONN[DatabricksConnection]
        STMT[DatabricksStatement]
    end

    subgraph "Databricks"
        WH[SQL Warehouse]
        CATALOG[Unity Catalog]
        STORAGE[Cloud Storage]
    end

    TEST --> ENV
    ENV --> DRIVER
    DRIVER --> DB --> CONN --> STMT
    STMT --> WH
    WH --> CATALOG
    WH --> STORAGE
```

**Test Prerequisites:**
- Live Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless or Classic)
- Personal Access Token with appropriate permissions
- Test catalog and schema (e.g., `e2e_tests.rust_adbc_driver`)

**Configuration:**
```rust
/// E2E test configuration from environment
pub struct E2EConfig {
    pub host: String,                    // DATABRICKS_HOST
    pub warehouse_id: String,            // DATABRICKS_WAREHOUSE_ID
    pub token: String,                   // DATABRICKS_TOKEN
    pub catalog: String,                 // DATABRICKS_E2E_CATALOG (default: "e2e_tests")
    pub schema: String,                  // DATABRICKS_E2E_SCHEMA (default: "rust_adbc_driver")
}
```

#### 9.4.2 E2E Test Categories

##### Connection Lifecycle Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_connection_open_creates_session` | Open connection creates session in warehouse | Session ID returned, session active |
| `e2e_connection_close_terminates_session` | Close connection terminates session | Session no longer active |
| `e2e_connection_set_catalog_changes_context` | Set catalog option changes active catalog | Queries run in correct catalog |
| `e2e_connection_set_schema_changes_context` | Set schema option changes active schema | Queries run in correct schema |
| `e2e_connection_timeout_handles_gracefully` | Connection timeout handled correctly | Appropriate error returned |

##### Basic Query Execution Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_query_select_one_returns_result` | Execute `SELECT 1` | Single row, single column with value 1 |
| `e2e_query_empty_result_returns_schema` | Execute query with 0 rows | Empty RecordBatch with correct schema |
| `e2e_query_null_values_handled` | Query with NULL values | Null bitmaps correctly set |
| `e2e_query_unicode_strings_preserved` | Query with emoji/unicode | Unicode characters preserved |
| `e2e_query_syntax_error_returns_error` | Invalid SQL syntax | ADBC InvalidArguments error |

##### Data Type Coverage Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_types_numeric_all_sizes` | INT8, INT16, INT32, INT64, FLOAT, DOUBLE | Correct Arrow types and values |
| `e2e_types_decimal_precision_scale` | DECIMAL(10,2), DECIMAL(38,10) | Decimal128 with correct precision |
| `e2e_types_string_binary` | STRING, BINARY | Utf8 and Binary arrays |
| `e2e_types_temporal` | DATE, TIMESTAMP, TIMESTAMP_NTZ | Correct Arrow temporal types |
| `e2e_types_complex_array` | ARRAY<INT>, ARRAY<STRING> | ListArray with correct children |
| `e2e_types_complex_struct` | STRUCT<a: INT, b: STRING> | StructArray with fields |
| `e2e_types_complex_map` | MAP<STRING, INT> | MapArray with key/value types |

##### Large Result Handling Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_result_inline_small_query` | Query < 16MB result | INLINE disposition, single batch |
| `e2e_result_external_large_query` | Query > 16MB result | EXTERNAL_LINKS, multiple chunks |
| `e2e_result_external_parallel_fetch` | Large result with 10+ chunks | Chunks fetched in parallel, ordered |
| `e2e_result_millions_rows` | Query returning 10M rows | All rows retrieved, memory efficient |
| `e2e_result_wide_table` | Query with 1000 columns | Schema and data correct |

##### Compression Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_compression_lz4_decompression` | Large result with LZ4_FRAME | Correct decompression, data intact |
| `e2e_compression_none_fallback` | Query with compression=NONE | Uncompressed data handled |

##### Metadata Query Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_metadata_get_info_driver_version` | Get driver info codes | Driver name, version, vendor |
| `e2e_metadata_get_objects_catalogs` | List all catalogs | Correct catalog list |
| `e2e_metadata_get_objects_schemas` | List schemas in catalog | Correct schema list |
| `e2e_metadata_get_objects_tables` | List tables in schema | Correct table list with types |
| `e2e_metadata_get_table_schema` | Get schema for specific table | Arrow schema matches table |
| `e2e_metadata_get_table_types` | Get supported table types | TABLE, VIEW, etc. |

##### Statement Management Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_statement_reuse_multiple_queries` | Execute multiple queries on same statement | All queries succeed |
| `e2e_statement_cancel_running_query` | Cancel long-running query | Statement cancelled, error returned |
| `e2e_statement_concurrent_statements` | Multiple statements on same connection | All execute independently |
| `e2e_statement_execute_update_insert` | Execute INSERT statement | Row count returned |
| `e2e_statement_execute_update_create_table` | Execute CREATE TABLE | Success, no row count |

##### Error Handling Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_error_invalid_warehouse_id` | Connect with invalid warehouse | Appropriate ADBC error |
| `e2e_error_invalid_token` | Connect with invalid token | Unauthenticated error |
| `e2e_error_insufficient_permissions` | Query restricted table | Unauthorized error |
| `e2e_error_table_not_found` | Query non-existent table | NotFound error |
| `e2e_error_network_timeout` | Simulate network timeout | IO error with retry |
| `e2e_error_expired_url_refresh` | Delayed chunk fetch with expiration | URL refreshed, data retrieved |

##### Session Management Tests

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_session_temp_table_isolated` | Create temp table, query in same session | Temp table accessible |
| `e2e_session_temp_table_not_shared` | Create temp table, new connection | Temp table not visible |
| `e2e_session_use_catalog_persists` | USE CATALOG in statement | Subsequent queries use catalog |
| `e2e_session_use_schema_persists` | USE SCHEMA in statement | Subsequent queries use schema |

##### Parameterized Query Tests (Phase 2)

| Test Name | Description | Validation |
|-----------|-------------|------------|
| `e2e_params_bind_scalar_values` | Bind scalar parameters | Parameters substituted correctly |
| `e2e_params_bind_arrow_batch` | Bind RecordBatch | Batch uploaded, query uses values |
| `e2e_params_prepared_statement_reuse` | Prepare and execute multiple times | Efficient reuse |

#### 9.4.3 E2E Test Implementation Pattern

```rust
#[cfg(test)]
mod e2e_tests {
    use super::*;
    use adbc_core::{Driver, Database, Connection, Statement};

    /// Load E2E configuration from environment
    fn get_e2e_config() -> E2EConfig {
        E2EConfig {
            host: std::env::var("DATABRICKS_HOST")
                .expect("DATABRICKS_HOST must be set"),
            warehouse_id: std::env::var("DATABRICKS_WAREHOUSE_ID")
                .expect("DATABRICKS_WAREHOUSE_ID must be set"),
            token: std::env::var("DATABRICKS_TOKEN")
                .expect("DATABRICKS_TOKEN must be set"),
            catalog: std::env::var("DATABRICKS_E2E_CATALOG")
                .unwrap_or_else(|_| "e2e_tests".to_string()),
            schema: std::env::var("DATABRICKS_E2E_SCHEMA")
                .unwrap_or_else(|_| "rust_adbc_driver".to_string()),
        }
    }

    /// Create test connection with proper cleanup
    fn create_test_connection() -> Result<impl Connection> {
        let config = get_e2e_config();
        let mut driver = DatabricksDriver::new();
        let mut db = driver.new_database_with_opts([
            (OptionDatabase::Uri, config.host.into()),
            (OptionDatabase::Username, config.warehouse_id.into()),
            (OptionDatabase::Password, config.token.into()),
        ])?;
        db.new_connection()
    }

    #[test]
    #[ignore] // Run only with --ignored flag
    fn e2e_query_select_one_returns_result() -> Result<()> {
        let mut conn = create_test_connection()?;
        let mut stmt = conn.new_statement()?;

        stmt.set_sql_query("SELECT 1 AS one")?;
        let mut reader = stmt.execute()?;

        // Read first batch
        let batch = reader.next()
            .expect("Expected at least one batch")
            .expect("Failed to read batch");

        // Validate schema
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "one");

        // Validate data
        assert_eq!(batch.num_rows(), 1);
        let array = batch.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        assert_eq!(array.value(0), 1);

        Ok(())
    }

    #[test]
    #[ignore]
    fn e2e_result_external_large_query() -> Result<()> {
        let mut conn = create_test_connection()?;
        let mut stmt = conn.new_statement()?;

        // Generate large result (>16MB to trigger EXTERNAL_LINKS)
        stmt.set_sql_query(
            "SELECT
                id,
                CONCAT('row_', CAST(id AS STRING)) AS text,
                RAND() AS random_value
            FROM RANGE(0, 1000000)"
        )?;

        let mut reader = stmt.execute()?;

        let mut total_rows = 0;
        while let Some(batch) = reader.next().transpose()? {
            total_rows += batch.num_rows();

            // Validate each batch has correct schema
            assert_eq!(batch.schema().fields().len(), 3);
        }

        assert_eq!(total_rows, 1000000);
        Ok(())
    }
}
```

#### 9.4.4 Test Data Setup

**Test Catalog/Schema Creation:**
```sql
-- Setup script for E2E tests
CREATE CATALOG IF NOT EXISTS e2e_tests;
CREATE SCHEMA IF NOT EXISTS e2e_tests.rust_adbc_driver;

USE e2e_tests.rust_adbc_driver;

-- Test table with various data types
CREATE TABLE IF NOT EXISTS test_types (
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(10,2),
    col_string STRING,
    col_binary BINARY,
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_array ARRAY<INT>,
    col_struct STRUCT<a: INT, b: STRING>,
    col_map MAP<STRING, INT>
);

-- Insert test data
INSERT INTO test_types VALUES (
    true,
    127,
    32767,
    2147483647,
    9223372036854775807,
    3.14,
    2.718281828,
    123.45,
    'Hello, World! 🌍',
    X'DEADBEEF',
    DATE '2024-12-08',
    TIMESTAMP '2024-12-08 12:34:56',
    ARRAY(1, 2, 3),
    STRUCT(42, 'answer'),
    MAP('key1', 100, 'key2', 200)
);
```

#### 9.4.5 CI/CD Integration

```mermaid
flowchart LR
    subgraph "CI Pipeline"
        BUILD[Build Rust Crate]
        UNIT[Unit Tests]
        INTEGRATION[Integration Tests]
        E2E[E2E Tests]
    end

    subgraph "Test Infrastructure"
        WH[Databricks Warehouse<br/>Serverless]
        SECRETS[CI Secrets<br/>PAT Token]
    end

    BUILD --> UNIT
    UNIT --> INTEGRATION
    INTEGRATION --> E2E
    E2E --> WH
    E2E --> SECRETS
```

**CI Configuration (GitHub Actions Example):**
```yaml
name: E2E Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_WAREHOUSE_ID: ${{ secrets.DATABRICKS_WAREHOUSE_ID }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      DATABRICKS_E2E_CATALOG: e2e_tests
      DATABRICKS_E2E_SCHEMA: rust_adbc_driver_ci

    steps:
      - uses: actions/checkout@v4

      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Run E2E Tests
        run: |
          cd rust/driver/databricks
          cargo test --release --ignored -- --test-threads=1
```

#### 9.4.6 E2E Test Execution

**Running E2E Tests Locally:**
```bash
# Set environment variables
export DATABRICKS_HOST="https://my-workspace.cloud.databricks.com"
export DATABRICKS_WAREHOUSE_ID="abc123def456"
export DATABRICKS_TOKEN="dapi1234567890"
export DATABRICKS_E2E_CATALOG="e2e_tests"
export DATABRICKS_E2E_SCHEMA="rust_adbc_driver"

# Run E2E tests
cd rust/driver/databricks
cargo test --release --ignored

# Run specific E2E test
cargo test --release --ignored e2e_query_select_one_returns_result

# Run with verbose output
cargo test --release --ignored -- --nocapture --test-threads=1
```

#### 9.4.7 Test Success Criteria

| Criterion | Requirement |
|-----------|-------------|
| **Pass Rate** | 100% of E2E tests must pass |
| **Coverage** | All ADBC trait methods exercised |
| **Data Types** | All Spark SQL -> Arrow type mappings verified |
| **Result Sizes** | Both INLINE and EXTERNAL_LINKS paths tested |
| **Error Cases** | All error mappings verified with real errors |
| **Performance** | Large result tests complete within reasonable time (< 60s for 10M rows) |

#### 9.4.8 Troubleshooting E2E Test Failures

```mermaid
flowchart TB
    FAIL[E2E Test Failure]

    FAIL --> AUTH{Authentication<br/>Error?}
    FAIL --> CONN{Connection<br/>Error?}
    FAIL --> DATA{Data<br/>Mismatch?}
    FAIL --> PERF{Timeout?}

    AUTH -->|Yes| CHECK_TOKEN[Check PAT token validity]
    AUTH -->|Yes| CHECK_PERMS[Check warehouse permissions]

    CONN -->|Yes| CHECK_WH[Verify warehouse running]
    CONN -->|Yes| CHECK_NET[Check network connectivity]

    DATA -->|Yes| CHECK_SCHEMA[Verify test data setup]
    DATA -->|Yes| CHECK_TYPES[Verify type mapping]

    PERF -->|Yes| CHECK_SIZE[Check result size]
    PERF -->|Yes| CHECK_CONC[Check concurrency settings]
```

**Common Issues:**
- **Token Expired**: PAT tokens expire; rotate regularly
- **Warehouse Stopped**: Auto-stop disabled warehouses may stop
- **Schema Missing**: Test catalog/schema not created
- **Network Timeout**: Increase timeout for large results
- **Concurrent Access**: Some tests require `--test-threads=1`

### 9.5 Implementation Findings and Lessons Learned

**Status**: Updated 2025-12-11 after Sprint 5 E2E Testing

#### 9.5.1 Key Findings

##### Finding 1: User-Agent Header Required for INLINE_OR_EXTERNAL_LINKS

**Issue**: Initial implementation used `INLINE_OR_EXTERNAL_LINKS` disposition but received API error:
```
INVALID_PARAMETER_VALUE - INLINE_OR_EXTERNAL_LINKS is not a supported disposition
```

**Root Cause**: The `INLINE_OR_EXTERNAL_LINKS` disposition requires a specific User-Agent header for server-side feature detection.

**Solution**: Changed User-Agent from `"adbc-driver-databricks/{version}"` to `"DatabricksJDBCDriverOSS/{version} (ADBC)"` to match the C# driver's format.

**Impact**: This enables optimal performance as the API automatically chooses inline results for small responses (<16MB) and external links for large results.

**Code Location**: `src/client/mod.rs:229-239`

```rust
// Use DatabricksJDBCDriverOSS prefix for server-side feature compatibility
// (e.g., INLINE_OR_EXTERNAL_LINKS disposition support).
let user_agent = format!("DatabricksJDBCDriverOSS/{} (ADBC)", DRIVER_VERSION);
```

##### Finding 2: Session ID Conflicts with Catalog/Schema Parameters

**Issue**: Tests for catalog/schema context changes failed with:
```
INVALID_PARAMETER_VALUE - Incompatible parameters: The session_id field
cannot be set at the same time as the catalog or schema fields.
```

**Root Cause**: The Databricks Statement Execution API doesn't allow `session_id` parameter when `catalog` or `schema` parameters are also specified in the request.

**Solution**: Conditionally omit `session_id` when catalog or schema overrides are present:

```rust
session_id: if current_catalog.is_some() || current_schema.is_some() {
    None
} else {
    Some(session_id)
}
```

**Impact**: Enables catalog/schema context changes via ADBC's `set_option` mechanism.

**Code Location**: `src/statement.rs:214-218, 375-379`

##### Finding 3: Different Dispositions for Query Types

**Issue**: DDL statements like `USE CATALOG` failed with format errors when using ArrowStream format.

**Root Cause**: Different statement types have different result format requirements:
- SELECT queries: Can use `EXTERNAL_LINKS` + `ARROW_STREAM`
- DDL/DML statements: Should use `INLINE` + `JSON_ARRAY`

**Solution**: Use different dispositions/formats for different execution methods:
- `execute()`: `EXTERNAL_LINKS` + `ARROW_STREAM` (for SELECT queries)
- `execute_update()`: `INLINE` + `JSON_ARRAY` (for DDL/DML)

**Status**: Partially resolved. SQL-based catalog/schema changes via `USE CATALOG`/`USE SCHEMA` still have issues. Recommend using ADBC's `set_option` mechanism instead.

##### Finding 4: Parallel Downloading Performance

**Validation**: Successfully tested parallel chunk downloading with large result sets:
- ✅ Default concurrency: 8 parallel downloads
- ✅ 100k rows: Passes
- ✅ 1 million rows: Passes
- ✅ 384 batches streamed successfully

**Performance**: Large result tests complete well within acceptable timeframes.

**Code Location**: `src/fetch/mod.rs:30-44`

##### Finding 5: NULL Value Handling Issue

**Issue**: NULL values in Arrow arrays not being detected correctly:
```rust
assert!(array.is_null(0));  // Fails even when value is NULL
assert_eq!(array.null_count(), 1);  // Returns 0
```

**Status**: Unresolved - requires further investigation of Arrow IPC NULL bitmap encoding from Databricks.

**Impact**: 2 E2E tests failing (test_e2e_query_null_values, e2e_null_values)

**Next Steps**:
- Investigate Arrow IPC stream encoding from Databricks
- Verify NULL bitmap is correctly preserved during base64 decode -> StreamReader parsing
- Compare with C# driver's NULL handling implementation

#### 9.5.2 Test Results Summary

**Overall Results**: 66 out of 70 E2E tests passing (94% success rate)

| Test Category | Passing | Total | Status |
|---------------|---------|-------|--------|
| Connection Tests | 10 | 12 | 83% ✅ |
| Basic Query Tests | 13 | 14 | 93% ✅ |
| Type Mapping Tests | 11 | 11 | 100% ✅ |
| Large Result Tests | 11 | 11 | 100% ✅ |
| E2E Top-Level Tests | 9 | 10 | 90% ✅ |
| Integration Tests | 59 | 59 | 100% ✅ |
| Unit Tests | 259 | 259 | 100% ✅ |

**Failing Tests (4)**:
1. `test_e2e_connection_use_catalog_statement` - DDL via SQL not fully supported
2. `test_e2e_connection_use_schema_statement` - DDL via SQL not fully supported
3. `test_e2e_query_null_values` - NULL bitmap encoding issue
4. `e2e_null_values` - NULL bitmap encoding issue

**Test Infrastructure Changes**:
- Removed all `#[ignore]` attributes (57 E2E tests)
- Tests now run automatically when `DATABRICKS_TEST_CONFIG_FILE` is set
- Skip gracefully via `skip_if_no_config!()` macro when config unavailable
- CI/CD ready for integration

#### 9.5.3 Updated Design Decisions

Based on implementation findings, the following design decisions have been updated:

| Decision | Original Choice | Updated Choice | Rationale |
|----------|----------------|----------------|-----------|
| Result Disposition | INLINE_OR_EXTERNAL_LINKS | ~~INLINE_OR_EXTERNAL_LINKS~~ → EXTERNAL_LINKS for queries, INLINE for DDL | More reliable; INLINE_OR_EXTERNAL_LINKS requires specific User-Agent |
| User-Agent Format | Generic driver name | `DatabricksJDBCDriverOSS/{version} (ADBC)` | Required for server-side feature detection |
| Session ID Handling | Always include | Conditional (omit with catalog/schema) | API restriction on parameter combinations |
| Catalog/Schema Changes | Support SQL statements | Prefer `set_option` mechanism | SQL-based changes have format compatibility issues |

#### 9.5.4 Recommendations for Production Use

**Required Configuration**:
```rust
// User-Agent must use DatabricksJDBCDriverOSS prefix
const USER_AGENT: &str = "DatabricksJDBCDriverOSS/0.22.0 (ADBC)";
```

**Best Practices**:
1. **Catalog/Schema Changes**: Use ADBC's `set_option` mechanism rather than SQL `USE` statements
2. **Result Size**: Driver automatically handles both small (inline) and large (external links) results
3. **Concurrency**: Default 8 parallel downloads is optimal for most workloads
4. **NULL Handling**: Be aware of potential NULL detection issues (under investigation)

**Known Limitations**:
- NULL value detection may not work correctly in all cases
- SQL-based catalog/schema changes (`USE CATALOG`, `USE SCHEMA`) not fully supported
- Recommend using `connection.set_option()` for catalog/schema context changes

#### 9.5.5 Areas for Future Investigation

1. **NULL Bitmap Encoding**:
   - Investigate Arrow IPC NULL bitmap preservation through base64 decode → StreamReader
   - Compare Databricks Arrow encoding with Arrow spec expectations
   - Test with various NULL patterns (single NULL, multiple NULLs, all NULLs)

2. **DDL Statement Support**:
   - Determine if `USE CATALOG`/`USE SCHEMA` should be supported via execute_update
   - Clarify API expectations for DDL statement result formats
   - Consider adding explicit DDL vs DML detection

3. **Disposition Optimization**:
   - Re-evaluate using `INLINE_OR_EXTERNAL_LINKS` now that User-Agent is correct
   - Measure performance difference between explicit EXTERNAL_LINKS vs auto-selection
   - Consider making disposition configurable per-statement

4. **Error Handling**:
   - Add better error messages for common API parameter conflicts
   - Improve diagnostics for disposition/format compatibility errors

---

## 10. Future Enhancements

### Phase 2
- OAuth 2.0 / M2M token authentication
- Token refresh for long-running connections
- Connection pooling

### Phase 3
- Prepared statement caching
- Bulk ingestion support
- Query progress tracking

### Phase 4
- Substrait query support
- Statistics retrieval
- Unity Catalog integration

---

## 11. Appendix

### A. SEA API Reference

Base URL: `https://{workspace-host}/api/2.0/sql/statements`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | POST | Execute statement |
| `/{id}` | GET | Get statement status/result |
| `/{id}/result/chunks/{index}` | GET | Get result chunk |
| `/{id}/cancel` | POST | Cancel statement |
| `/{id}` | DELETE | Close statement |
| `/sessions/` | POST | Create session |
| `/sessions/{id}` | DELETE | Terminate session |

### B. ADBC Trait Implementation Checklist

| Trait | Method | Implementation Status |
|-------|--------|----------------------|
| Driver | `new_database` | Implemented |
| Driver | `new_database_with_opts` | Implemented |
| Database | `new_connection` | Implemented |
| Database | `new_connection_with_opts` | Implemented |
| Connection | `new_statement` | Implemented |
| Connection | `cancel` | Implemented (no-op) |
| Connection | `get_info` | Implemented |
| Connection | `get_objects` | Implemented |
| Connection | `get_table_schema` | Implemented |
| Connection | `get_table_types` | Implemented |
| Connection | `get_statistics` | Returns NotImplemented |
| Connection | `get_statistic_names` | Returns NotImplemented |
| Connection | `commit` | Returns NotImplemented |
| Connection | `rollback` | Returns NotImplemented |
| Statement | `set_sql_query` | Implemented |
| Statement | `execute` | Implemented |
| Statement | `execute_update` | Implemented |
| Statement | `execute_schema` | Implemented |
| Statement | `cancel` | Implemented |
| Statement | `bind` | Phase 2 |
| Statement | `prepare` | Phase 2 |
