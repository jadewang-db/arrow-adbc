# Databricks Rust ADBC Driver Design

**Version**: 1.0
**Last Updated**: 2024-12-08
**Author**: PECO Team
**Status**: Draft

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
| Driver | `new_database` | Required |
| Driver | `new_database_with_opts` | Required |
| Database | `new_connection` | Required |
| Database | `new_connection_with_opts` | Required |
| Connection | `new_statement` | Required |
| Connection | `cancel` | Required |
| Connection | `get_info` | Required |
| Connection | `get_objects` | Required |
| Connection | `get_table_schema` | Required |
| Connection | `get_table_types` | Required |
| Connection | `commit` | Returns NotImplemented |
| Connection | `rollback` | Returns NotImplemented |
| Statement | `set_sql_query` | Required |
| Statement | `execute` | Required |
| Statement | `execute_update` | Required |
| Statement | `execute_schema` | Required |
| Statement | `cancel` | Required |
| Statement | `bind` | Phase 2 |
| Statement | `prepare` | Phase 2 |
