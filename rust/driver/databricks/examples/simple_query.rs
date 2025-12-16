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
//! Simple example showing the minimal code needed to query Databricks.
//!
//! # Usage
//!
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
//! export DATABRICKS_TOKEN="dapi_your_token"
//! cargo run --example simple_query
//! ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_databricks::DatabricksDriver;
use arrow_array::RecordBatchReader;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get configuration from environment
    let host = env::var("DATABRICKS_HOST")?;
    let warehouse_id = env::var("DATABRICKS_WAREHOUSE_ID")?;
    let token = env::var("DATABRICKS_TOKEN")?;

    println!("Connecting to Databricks...");

    // Create driver and database
    let mut driver = DatabricksDriver::new();
    let db = driver.new_database_with_opts([
        (OptionDatabase::Uri, OptionValue::String(host)),
        (OptionDatabase::Password, OptionValue::String(token)),
        (
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id),
        ),
    ])?;

    // Create connection
    let mut conn = db.new_connection()?;
    println!("Connected! Session: {:?}", conn.session_id());

    // Execute query
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query("SELECT 'Hello from Databricks!' AS message, 42 AS answer")?;

    println!("\nExecuting query...");
    let reader = stmt.execute()?;

    // Print schema
    let schema = reader.schema();
    println!("\nSchema:");
    for field in schema.fields() {
        println!("  - {}: {:?}", field.name(), field.data_type());
    }

    // Read and print results
    println!("\nResults:");
    for batch_result in reader {
        let batch = batch_result?;
        println!("  Batch: {} rows × {} columns", batch.num_rows(), batch.num_columns());

        // Print each row (simplified output)
        for row in 0..batch.num_rows() {
            print!("  Row {}: ", row);
            for col in 0..batch.num_columns() {
                print!("{:?} ", batch.column(col));
            }
            println!();
        }
    }

    println!("\nDone!");
    Ok(())
}
