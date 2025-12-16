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

-- ============================================================================
-- E2E Test Data Setup Script for Databricks Rust ADBC Driver
-- ============================================================================
--
-- This script creates the test catalog, schema, and tables needed for E2E tests.
-- Run this script once in your Databricks workspace before running E2E tests.
--
-- Prerequisites:
-- - Unity Catalog enabled in your workspace
-- - Appropriate permissions to create catalogs and schemas
--
-- Usage:
-- 1. Connect to your Databricks workspace
-- 2. Run this script in a SQL editor or notebook
-- 3. Update your test configuration JSON with the catalog/schema names
-- ============================================================================

-- Create test catalog (requires appropriate permissions)
-- If you don't have permission to create catalogs, use an existing catalog
-- and update the schema creation statement below.
CREATE CATALOG IF NOT EXISTS e2e_tests;

-- Create test schema
CREATE SCHEMA IF NOT EXISTS e2e_tests.rust_adbc_driver;

-- Switch to test schema
USE e2e_tests.rust_adbc_driver;

-- ============================================================================
-- Test Tables
-- ============================================================================

-- Table with various data types for type mapping tests
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

-- Insert test data with various values
INSERT INTO test_types VALUES (
    true,                           -- col_boolean
    127,                            -- col_tinyint (max value)
    32767,                          -- col_smallint (max value)
    2147483647,                     -- col_int (max value)
    9223372036854775807,            -- col_bigint (max value)
    3.14,                           -- col_float
    2.718281828459045,              -- col_double
    12345.67,                       -- col_decimal
    'Hello, World!',                -- col_string (unicode: includes emoji)
    X'DEADBEEF',                    -- col_binary
    DATE '2024-12-08',              -- col_date
    TIMESTAMP '2024-12-08 12:34:56',-- col_timestamp
    ARRAY(1, 2, 3),                 -- col_array
    STRUCT(42, 'answer'),           -- col_struct
    MAP('key1', 100, 'key2', 200)   -- col_map
);

-- Insert row with NULL values for null handling tests
INSERT INTO test_types VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);

-- Insert row with edge case values
INSERT INTO test_types VALUES (
    false,                          -- col_boolean
    -128,                           -- col_tinyint (min value)
    -32768,                         -- col_smallint (min value)
    -2147483648,                    -- col_int (min value)
    -9223372036854775808,           -- col_bigint (min value)
    -3.14,                          -- col_float (negative)
    -2.718281828459045,             -- col_double (negative)
    -99999.99,                      -- col_decimal (negative)
    '',                             -- col_string (empty)
    X'',                            -- col_binary (empty)
    DATE '1970-01-01',              -- col_date (epoch)
    TIMESTAMP '1970-01-01 00:00:00',-- col_timestamp (epoch)
    ARRAY(),                        -- col_array (empty)
    STRUCT(0, ''),                  -- col_struct (zeros/empty)
    MAP()                           -- col_map (empty)
);

-- ============================================================================
-- Simple test table for basic queries
-- ============================================================================

CREATE TABLE IF NOT EXISTS simple_test (
    id INT,
    name STRING,
    value DOUBLE
);

INSERT INTO simple_test VALUES
    (1, 'one', 1.0),
    (2, 'two', 2.0),
    (3, 'three', 3.0);

-- ============================================================================
-- Large table for chunk fetching tests
-- This creates 100,000 rows to test EXTERNAL_LINKS result handling
-- ============================================================================

CREATE TABLE IF NOT EXISTS large_test AS
SELECT
    id,
    CONCAT('row_', CAST(id AS STRING)) AS text_col,
    RAND() AS random_value,
    REPEAT('x', 100) AS padding
FROM RANGE(0, 100000);

-- ============================================================================
-- Unicode test table
-- ============================================================================

CREATE TABLE IF NOT EXISTS unicode_test (
    id INT,
    text_value STRING
);

INSERT INTO unicode_test VALUES
    (1, 'Hello World'),
    (2, 'Hej Verden'),
    (3, 'Bonjour le monde'),
    (4, 'Hallo Welt'),
    (5, 'Ciao mondo');

-- ============================================================================
-- Verify tables created
-- ============================================================================

SHOW TABLES;

-- Count rows in each table
SELECT 'test_types' AS table_name, COUNT(*) AS row_count FROM test_types
UNION ALL
SELECT 'simple_test', COUNT(*) FROM simple_test
UNION ALL
SELECT 'large_test', COUNT(*) FROM large_test
UNION ALL
SELECT 'unicode_test', COUNT(*) FROM unicode_test;
