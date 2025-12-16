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
//! Advanced example demonstrating table operations with Databricks.
//!
//! This example shows:
//! - Creating a temporary table
//! - Inserting data
//! - Querying the data
//! - Updating records
//! - Deleting records
//! - Dropping the table
//!
//! # Usage
//!
//! ```bash
//! export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
//! export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
//! export DATABRICKS_TOKEN="dapi_your_token"
//! export DATABRICKS_CATALOG="main"      # Required for this example
//! export DATABRICKS_SCHEMA="default"    # Required for this example
//! cargo run --example table_operations
//! ```

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_databricks::DatabricksDriver;
use arrow_array::RecordBatchReader;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Table Operations Demo ===\n");

    // Get configuration
    let host = env::var("DATABRICKS_HOST")?;
    let warehouse_id = env::var("DATABRICKS_WAREHOUSE_ID")?;
    let token = env::var("DATABRICKS_TOKEN")?;
    let catalog = env::var("DATABRICKS_CATALOG")
        .expect("DATABRICKS_CATALOG required for this example");
    let schema = env::var("DATABRICKS_SCHEMA")
        .expect("DATABRICKS_SCHEMA required for this example");

    // Connect to Databricks
    println!("1. Connecting to Databricks...");
    let mut driver = DatabricksDriver::new();
    let db = driver.new_database_with_opts([
        (OptionDatabase::Uri, OptionValue::String(host)),
        (OptionDatabase::Password, OptionValue::String(token)),
        (
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id),
        ),
        (
            OptionDatabase::Other("databricks.catalog".into()),
            OptionValue::String(catalog.clone()),
        ),
        (
            OptionDatabase::Other("databricks.schema".into()),
            OptionValue::String(schema.clone()),
        ),
    ])?;

    let mut conn = db.new_connection()?;
    println!("   Connected! Using {}.{}", catalog, schema);
    println!();

    // Generate a unique table name for this run
    let table_name = format!("demo_users_{}", rand::random::<u32>());
    println!("2. Creating temporary table: {}", table_name);

    // Create table
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {}.{}.{} (\
            id INT, \
            name STRING, \
            email STRING, \
            age INT, \
            created_at TIMESTAMP\
        ) USING DELTA",
        catalog, schema, table_name
    );

    execute_update(&mut conn, &create_sql)?;
    println!("   Table created successfully");
    println!();

    // Insert data
    println!("3. Inserting sample data...");
    let insert_sql = format!(
        "INSERT INTO {}.{}.{} VALUES \
            (1, 'Alice Johnson', 'alice@example.com', 30, current_timestamp()), \
            (2, 'Bob Smith', 'bob@example.com', 25, current_timestamp()), \
            (3, 'Charlie Brown', 'charlie@example.com', 35, current_timestamp()), \
            (4, 'Diana Prince', 'diana@example.com', 28, current_timestamp())",
        catalog, schema, table_name
    );

    let rows = execute_update(&mut conn, &insert_sql)?;
    println!("   Inserted {} rows", rows);
    println!();

    // Query all data
    println!("4. Querying all data...");
    let query_sql = format!(
        "SELECT id, name, email, age FROM {}.{}.{} ORDER BY id",
        catalog, schema, table_name
    );
    execute_query(&mut conn, &query_sql)?;
    println!();

    // Update data
    println!("5. Updating age for user with id=2...");
    let update_sql = format!(
        "UPDATE {}.{}.{} SET age = 26 WHERE id = 2",
        catalog, schema, table_name
    );
    let rows = execute_update(&mut conn, &update_sql)?;
    println!("   Updated {} row(s)", rows);
    println!();

    // Query updated data
    println!("6. Querying updated data...");
    execute_query(&mut conn, &query_sql)?;
    println!();

    // Delete data
    println!("7. Deleting user with id=1...");
    let delete_sql = format!(
        "DELETE FROM {}.{}.{} WHERE id = 1",
        catalog, schema, table_name
    );
    let rows = execute_update(&mut conn, &delete_sql)?;
    println!("   Deleted {} row(s)", rows);
    println!();

    // Query final data
    println!("8. Querying remaining data...");
    execute_query(&mut conn, &query_sql)?;
    println!();

    // Get table metadata
    println!("9. Getting table schema...");
    match conn.get_table_schema(Some(&catalog), Some(&schema), &table_name) {
        Ok(schema) => {
            println!("   Table schema:");
            for field in schema.fields() {
                println!("     - {}: {:?}", field.name(), field.data_type());
            }
        }
        Err(e) => {
            println!("   Could not get schema: {}", e.message);
        }
    }
    println!();

    // Drop table
    println!("10. Cleaning up: dropping table...");
    let drop_sql = format!("DROP TABLE IF EXISTS {}.{}.{}", catalog, schema, table_name);
    execute_update(&mut conn, &drop_sql)?;
    println!("   Table dropped successfully");
    println!();

    println!("=== Demo completed successfully! ===");
    Ok(())
}

/// Execute a DML statement (INSERT, UPDATE, DELETE) and return affected rows
fn execute_update(conn: &mut impl Connection, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;

    match stmt.execute_update() {
        Ok(rows) => Ok(rows.unwrap_or(-1)),
        Err(e) => {
            eprintln!("   Error executing update: {}", e.message);
            Err(Box::new(e))
        }
    }
}

/// Execute a query and print results
fn execute_query(conn: &mut impl Connection, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;

    let reader = stmt.execute()?;
    let schema = reader.schema();

    // Print column headers
    print!("   ");
    for field in schema.fields() {
        print!("{:20} ", field.name());
    }
    println!();
    print!("   ");
    for _ in 0..schema.fields().len() {
        print!("{:20} ", "--------------------");
    }
    println!();

    // Print rows
    let mut row_count = 0;
    for batch_result in reader {
        let batch = batch_result?;
        row_count += batch.num_rows();

        // Simple row printing (for demo purposes)
        for _row_idx in 0..batch.num_rows() {
            print!("   ");
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                // Simple debug output - in production you'd format each type properly
                let value = format!("{:?}", col);
                // Extract just the value part (simplified)
                let display = value
                    .split('[')
                    .nth(1)
                    .and_then(|s| s.split(']').next())
                    .unwrap_or(&value);
                print!("{:20} ", display);
            }
            println!();
        }
    }

    println!("   Total rows: {}", row_count);
    Ok(())
}
