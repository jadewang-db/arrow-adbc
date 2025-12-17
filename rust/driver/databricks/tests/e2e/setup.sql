-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- E2E Test Data Setup Script
-- This script creates test catalog, schema, and tables for E2E testing
-- Run this script manually in your Databricks SQL Warehouse before running E2E tests

-- ========================================
-- Setup Test Catalog and Schema
-- ========================================

-- Create test catalog if it doesn't exist
-- Note: Adjust the catalog name as needed for your environment
CREATE CATALOG IF NOT EXISTS e2e_tests;

-- Create test schema
CREATE SCHEMA IF NOT EXISTS e2e_tests.rust_adbc_driver;

-- Set the default catalog and schema
USE CATALOG e2e_tests;
USE SCHEMA rust_adbc_driver;

-- ========================================
-- Test Table: test_types
-- ========================================
-- This table contains all supported data types for comprehensive type testing

DROP TABLE IF EXISTS test_types;

CREATE TABLE test_types (
    -- Numeric types
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(10,2),

    -- String and binary types
    col_string STRING,
    col_binary BINARY,

    -- Temporal types
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_timestamp_ntz TIMESTAMP_NTZ,

    -- Complex types
    col_array ARRAY<INT>,
    col_struct STRUCT<a: INT, b: STRING>,
    col_map MAP<STRING, INT>
);

-- Insert test data with various values
INSERT INTO test_types VALUES (
    -- Numeric types
    true,                         -- col_boolean
    127,                          -- col_tinyint
    32767,                        -- col_smallint
    2147483647,                   -- col_int
    9223372036854775807,          -- col_bigint
    3.14,                         -- col_float
    2.718281828,                  -- col_double
    123.45,                       -- col_decimal

    -- String and binary types
    'Hello, World! 🌍',           -- col_string
    X'DEADBEEF',                  -- col_binary

    -- Temporal types
    DATE '2024-12-08',            -- col_date
    TIMESTAMP '2024-12-08 12:34:56',  -- col_timestamp
    TIMESTAMP_NTZ '2024-12-08 12:34:56',  -- col_timestamp_ntz

    -- Complex types
    ARRAY(1, 2, 3),               -- col_array
    STRUCT(42, 'answer'),         -- col_struct
    MAP('key1', 100, 'key2', 200) -- col_map
);

-- Insert NULL values
INSERT INTO test_types VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);

-- Insert additional test data
INSERT INTO test_types VALUES (
    false, -128, -32768, -2147483648, -9223372036854775808,
    -1.5, -0.5, -999.99,
    'Test string with special chars: !@#$%^&*()', X'00FF',
    DATE '2000-01-01', TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP_NTZ '2000-01-01 00:00:00',
    ARRAY(10, 20, 30), STRUCT(0, 'zero'), MAP('a', 1, 'b', 2, 'c', 3)
);

-- ========================================
-- Test Table: test_large
-- ========================================
-- This table is used for testing large result sets (EXTERNAL_LINKS)

DROP TABLE IF EXISTS test_large;

CREATE TABLE test_large (
    id BIGINT,
    text STRING,
    random_value DOUBLE
);

-- Insert large dataset (adjust row count as needed)
-- This generates 1 million rows which should trigger EXTERNAL_LINKS
INSERT INTO test_large
SELECT
    id,
    CONCAT('row_', CAST(id AS STRING)) AS text,
    RAND() AS random_value
FROM RANGE(0, 1000000);

-- ========================================
-- Test Table: simple_table
-- ========================================
-- Simple table for basic query testing

DROP TABLE IF EXISTS simple_table;

CREATE TABLE simple_table (
    id INT,
    name STRING,
    value DOUBLE
);

INSERT INTO simple_table VALUES
    (1, 'Alice', 100.5),
    (2, 'Bob', 200.75),
    (3, 'Charlie', 300.25);

-- ========================================
-- Test View: simple_view
-- ========================================
-- View for metadata testing

DROP VIEW IF EXISTS simple_view;

CREATE VIEW simple_view AS
SELECT id, name FROM simple_table WHERE value > 150;

-- ========================================
-- Verification Queries
-- ========================================

-- Verify test_types
SELECT COUNT(*) AS test_types_count FROM test_types;

-- Verify test_large
SELECT COUNT(*) AS test_large_count FROM test_large;

-- Verify simple_table
SELECT COUNT(*) AS simple_table_count FROM simple_table;

-- Show all tables
SHOW TABLES IN e2e_tests.rust_adbc_driver;

-- ========================================
-- Cleanup (Optional)
-- ========================================
-- Uncomment the following to drop all test objects

-- DROP TABLE IF EXISTS e2e_tests.rust_adbc_driver.test_types;
-- DROP TABLE IF EXISTS e2e_tests.rust_adbc_driver.test_large;
-- DROP TABLE IF EXISTS e2e_tests.rust_adbc_driver.simple_table;
-- DROP VIEW IF EXISTS e2e_tests.rust_adbc_driver.simple_view;
-- DROP SCHEMA IF EXISTS e2e_tests.rust_adbc_driver;
-- DROP CATALOG IF EXISTS e2e_tests;
