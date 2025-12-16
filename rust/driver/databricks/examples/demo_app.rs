// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Demo application for Databricks Rust ADBC driver.
//!
//! This example demonstrates:
//! - Connecting to a Databricks SQL Warehouse
//! - Executing queries and reading results
//! - Working with Arrow RecordBatches
//! - Using catalog and schema
//! - Schema introspection
//! - Error handling
//!
//! # Usage
//!
//! Set environment variables:
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
//! export DATABRICKS_TOKEN="dapi_your_token_here"
//!
//! # Optional: Set default catalog and schema
//! export DATABRICKS_CATALOG="main"
//! export DATABRICKS_SCHEMA="default"
//! ```
//!
//! Run the demo:
//! ```bash
//! cargo run --example demo_app
//! ```

use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_databricks::DatabricksDriver;
use arrow_array::RecordBatchReader;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Databricks Rust ADBC Driver Demo ===\n");

    // Step 1: Load configuration from environment variables
    println!("Step 1: Loading configuration from environment variables...");
    let config = load_config()?;
    println!("  Host: {}", config.host);
    println!("  Warehouse ID: {}", config.warehouse_id);
    println!("  Token: {}...", &config.token[..10.min(config.token.len())]);
    if let Some(ref catalog) = config.catalog {
        println!("  Default Catalog: {}", catalog);
    }
    if let Some(ref schema) = config.schema {
        println!("  Default Schema: {}", schema);
    }
    println!();

    // Step 2: Create driver
    println!("Step 2: Creating Databricks ADBC driver...");
    let mut driver = DatabricksDriver::new();
    println!("  Driver created successfully");
    println!();

    // Step 3: Create database with configuration
    println!("Step 3: Creating database with connection options...");
    let mut db_opts = vec![
        (OptionDatabase::Uri, OptionValue::String(config.host.clone())),
        (
            OptionDatabase::Password,
            OptionValue::String(config.token.clone()),
        ),
        (
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(config.warehouse_id.clone()),
        ),
    ];

    // Add optional catalog and schema
    if let Some(ref catalog) = config.catalog {
        db_opts.push((
            OptionDatabase::Other("databricks.catalog".into()),
            OptionValue::String(catalog.clone()),
        ));
    }
    if let Some(ref schema) = config.schema {
        db_opts.push((
            OptionDatabase::Other("databricks.schema".into()),
            OptionValue::String(schema.clone()),
        ));
    }

    let db = driver.new_database_with_opts(db_opts)?;
    println!("  Database created successfully");
    println!();

    // Step 4: Create connection (creates session with Databricks)
    println!("Step 4: Creating connection to Databricks...");
    let mut conn = db.new_connection()?;

    // Get session ID to verify connection
    if let Some(session_id) = conn.session_id() {
        println!("  Connection established successfully");
        println!("  Session ID: {}", session_id);
    }

    // Print current catalog and schema
    if let Ok(catalog) = conn.get_option_string(OptionConnection::CurrentCatalog) {
        println!("  Current Catalog: {}", catalog);
    }
    if let Ok(schema) = conn.get_option_string(OptionConnection::CurrentSchema) {
        println!("  Current Schema: {}", schema);
    }
    println!();

    // Step 5: Execute a simple query
    println!("Step 5: Executing simple query...");
    run_simple_query(&mut conn)?;
    println!();

    // Step 6: Execute query with multiple types
    println!("Step 6: Executing query with various data types...");
    run_types_query(&mut conn)?;
    println!();

    // Step 7: Get table schema (if catalog/schema configured)
    if config.catalog.is_some() {
        println!("Step 7: Demonstrating schema introspection...");
        run_schema_introspection(&conn)?;
        println!();
    }

    // Step 8: Execute a query that returns multiple rows
    println!("Step 8: Executing query with multiple rows...");
    run_multi_row_query(&mut conn)?;
    println!();

    // Step 9: Demonstrate error handling
    println!("Step 9: Demonstrating error handling...");
    demonstrate_error_handling(&mut conn);
    println!();

    // Step 10: Clean up
    println!("Step 10: Cleaning up...");
    drop(conn);
    println!("  Connection closed, session terminated");
    println!();

    println!("=== Demo completed successfully! ===");
    Ok(())
}

/// Configuration for connecting to Databricks
struct Config {
    host: String,
    warehouse_id: String,
    token: String,
    catalog: Option<String>,
    schema: Option<String>,
}

/// Load configuration from environment variables
fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let host = env::var("DATABRICKS_HOST")
        .map_err(|_| "DATABRICKS_HOST environment variable not set")?;
    let warehouse_id = env::var("DATABRICKS_WAREHOUSE_ID")
        .map_err(|_| "DATABRICKS_WAREHOUSE_ID environment variable not set")?;
    let token = env::var("DATABRICKS_TOKEN")
        .map_err(|_| "DATABRICKS_TOKEN environment variable not set")?;

    // Optional catalog and schema
    let catalog = env::var("DATABRICKS_CATALOG").ok();
    let schema = env::var("DATABRICKS_SCHEMA").ok();

    Ok(Config {
        host,
        warehouse_id,
        token,
        catalog,
        schema,
    })
}

/// Execute a simple SELECT query
fn run_simple_query(conn: &mut impl Connection) -> Result<(), Box<dyn std::error::Error>> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query("SELECT 42 AS answer, 'Hello, Databricks!' AS message")?;

    println!("  Query: SELECT 42 AS answer, 'Hello, Databricks!' AS message");

    let reader = stmt.execute()?;
    let schema = reader.schema();

    println!("  Schema:");
    for field in schema.fields() {
        println!("    - {}: {:?}", field.name(), field.data_type());
    }

    println!("  Results:");
    for batch_result in reader {
        let batch = batch_result?;
        println!("    Batch with {} rows", batch.num_rows());

        // Print the batch using Arrow's display functionality
        for row_idx in 0..batch.num_rows() {
            print!("    Row {}: ", row_idx);
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                print!("{}: {:?} ", schema.field(col_idx).name(), col);
            }
            println!();
        }
    }

    Ok(())
}

/// Execute a query with various data types
fn run_types_query(conn: &mut impl Connection) -> Result<(), Box<dyn std::error::Error>> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(
        "SELECT \
            CAST(100 AS INT) AS int_val, \
            CAST(3.14159 AS DOUBLE) AS double_val, \
            'test string' AS string_val, \
            true AS bool_val, \
            CAST('2024-01-15' AS DATE) AS date_val"
    )?;

    println!("  Query: SELECT with various types (INT, DOUBLE, STRING, BOOLEAN, DATE)");

    let reader = stmt.execute()?;
    let schema = reader.schema();

    println!("  Schema:");
    for field in schema.fields() {
        println!("    - {}: {:?}", field.name(), field.data_type());
    }

    println!("  Results:");
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
    println!("    Total rows: {}", batches.iter().map(|b| b.num_rows()).sum::<usize>());

    Ok(())
}

/// Demonstrate schema introspection
fn run_schema_introspection(conn: &impl Connection) -> Result<(), Box<dyn std::error::Error>> {
    println!("  Getting schema for system.information_schema.tables...");

    match conn.get_table_schema(Some("system"), Some("information_schema"), "tables") {
        Ok(schema) => {
            println!("  Schema fields ({} total):", schema.fields().len());
            for (i, field) in schema.fields().iter().take(5).enumerate() {
                println!("    {}. {}: {:?}", i + 1, field.name(), field.data_type());
            }
            if schema.fields().len() > 5 {
                println!("    ... and {} more fields", schema.fields().len() - 5);
            }
        }
        Err(e) => {
            println!("  Note: Could not get table schema: {}", e.message);
            println!("  (This is expected if you don't have access to the system catalog)");
        }
    }

    Ok(())
}

/// Execute a query that returns multiple rows
fn run_multi_row_query(conn: &mut impl Connection) -> Result<(), Box<dyn std::error::Error>> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(
        "SELECT * FROM (VALUES \
            (1, 'Alice', 30), \
            (2, 'Bob', 25), \
            (3, 'Charlie', 35), \
            (4, 'Diana', 28), \
            (5, 'Eve', 32)) AS people(id, name, age)"
    )?;

    println!("  Query: SELECT from VALUES with 5 rows");

    let reader = stmt.execute()?;
    let schema = reader.schema();

    println!("  Schema:");
    for field in schema.fields() {
        println!("    - {}: {:?}", field.name(), field.data_type());
    }

    println!("  Results:");
    let mut total_rows = 0;
    let mut batch_count = 0;

    for batch_result in reader {
        let batch = batch_result?;
        batch_count += 1;
        total_rows += batch.num_rows();
        println!("    Batch {}: {} rows", batch_count, batch.num_rows());
    }

    println!("  Total: {} batches, {} rows", batch_count, total_rows);

    Ok(())
}

/// Demonstrate error handling
fn demonstrate_error_handling(conn: &mut impl Connection) {
    println!("  Attempting to query a non-existent table...");

    let mut stmt = match conn.new_statement() {
        Ok(s) => s,
        Err(e) => {
            println!("  Error creating statement: {}", e.message);
            return;
        }
    };

    if let Err(e) = stmt.set_sql_query("SELECT * FROM nonexistent_table_xyz_12345") {
        println!("  Error setting SQL: {}", e.message);
        return;
    }

    match stmt.execute() {
        Ok(_) => println!("  Unexpected: Query succeeded (table shouldn't exist)"),
        Err(e) => {
            println!("  Got expected error:");
            println!("    Status: {:?}", e.status);
            println!("    Message: {}", e.message);
        }
    }

    // Verify statement can still be used after error
    println!("  Verifying statement recovery...");
    if stmt.set_sql_query("SELECT 1 AS recovered").is_ok() {
        match stmt.execute() {
            Ok(reader) => {
                let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap_or_default();
                println!("    Statement recovered successfully, read {} batch(es)", batches.len());
            }
            Err(e) => println!("    Error during recovery: {}", e.message),
        }
    }
}
