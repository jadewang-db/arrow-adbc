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

-- =============================================================================
-- E2E Test Data Setup Script for Databricks Rust ADBC Driver
-- =============================================================================
--
-- This script creates the test catalog, schema, and tables needed for E2E tests.
-- Run this script once to set up your test environment.
--
-- Usage:
--   Option 1: databricks sql exec -f tests/e2e/setup.sql
--   Option 2: Copy and execute manually in a SQL Warehouse
--
-- Note: Modify the catalog and schema names if needed for your environment.
-- =============================================================================

-- Create test catalog (requires appropriate privileges)
CREATE CATALOG IF NOT EXISTS e2e_tests;

-- Create test schema
CREATE SCHEMA IF NOT EXISTS e2e_tests.rust_adbc_driver;

-- Set context
USE CATALOG e2e_tests;
USE SCHEMA rust_adbc_driver;

-- =============================================================================
-- Test table with various data types
-- =============================================================================
CREATE OR REPLACE TABLE test_types (
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
    true,                                    -- col_boolean
    127,                                     -- col_tinyint (max)
    32767,                                   -- col_smallint (max)
    2147483647,                              -- col_int (max)
    9223372036854775807,                     -- col_bigint (max)
    3.14,                                    -- col_float
    2.718281828,                             -- col_double
    123.45,                                  -- col_decimal
    'Hello, World!',                         -- col_string
    X'DEADBEEF',                             -- col_binary
    DATE '2024-12-08',                       -- col_date
    TIMESTAMP '2024-12-08 12:34:56',         -- col_timestamp
    ARRAY(1, 2, 3),                          -- col_array
    STRUCT(42, 'answer'),                    -- col_struct
    MAP('key1', 100, 'key2', 200)            -- col_map
);

-- Insert row with NULL values
INSERT INTO test_types VALUES (
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL
);

-- Insert row with minimum values
INSERT INTO test_types VALUES (
    false,                                   -- col_boolean
    -128,                                    -- col_tinyint (min)
    -32768,                                  -- col_smallint (min)
    -2147483648,                             -- col_int (min)
    -9223372036854775808,                    -- col_bigint (min)
    -3.14,                                   -- col_float
    -2.718281828,                            -- col_double
    -999.99,                                 -- col_decimal
    '',                                      -- col_string (empty)
    X'',                                     -- col_binary (empty)
    DATE '1970-01-01',                       -- col_date (epoch)
    TIMESTAMP '1970-01-01 00:00:00',         -- col_timestamp (epoch)
    ARRAY(),                                 -- col_array (empty)
    STRUCT(0, ''),                           -- col_struct
    MAP()                                    -- col_map (empty)
);

-- =============================================================================
-- Test table with decimal precision variations
-- =============================================================================
CREATE OR REPLACE TABLE test_decimals (
    dec_10_2 DECIMAL(10, 2),
    dec_18_4 DECIMAL(18, 4),
    dec_38_10 DECIMAL(38, 10),
    dec_38_0 DECIMAL(38, 0)
);

INSERT INTO test_decimals VALUES (
    12345678.90,
    12345678901234.5678,
    1234567890123456789012345678.1234567890,
    12345678901234567890123456789012345678
);

-- =============================================================================
-- Test table for string edge cases
-- =============================================================================
CREATE OR REPLACE TABLE test_strings (
    id INT,
    content STRING
);

INSERT INTO test_strings VALUES
    (1, 'simple'),
    (2, 'with spaces'),
    (3, 'with\ttab'),
    (4, 'with\nnewline'),
    (5, 'unicode: cafe'),
    (6, ''),
    (7, NULL);

-- =============================================================================
-- View for testing view metadata
-- =============================================================================
CREATE OR REPLACE VIEW test_view AS
SELECT col_int, col_string FROM test_types WHERE col_int IS NOT NULL;

-- =============================================================================
-- Verification queries
-- =============================================================================
SELECT 'test_types' AS table_name, COUNT(*) AS row_count FROM test_types
UNION ALL
SELECT 'test_decimals', COUNT(*) FROM test_decimals
UNION ALL
SELECT 'test_strings', COUNT(*) FROM test_strings;
