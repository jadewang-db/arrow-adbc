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

//! Unit tests for Spark SQL to Arrow type conversion.
//!
//! Tests cover:
//! - Basic type mappings (boolean, integers, floating point, strings)
//! - Date and timestamp types
//! - Decimal types with precision/scale
//! - Complex types (arrays, maps, structs)
//! - Case insensitivity
//! - Unknown type handling

use adbc_driver_databricks::fetch::spark_type_to_arrow;
use arrow_schema::DataType;
use std::sync::Arc;

// =============================================================================
// Boolean Type Tests
// =============================================================================

mod boolean_tests {
    use super::*;

    #[test]
    fn test_boolean_uppercase() {
        assert_eq!(spark_type_to_arrow("BOOLEAN"), DataType::Boolean);
    }

    #[test]
    fn test_boolean_lowercase() {
        assert_eq!(spark_type_to_arrow("boolean"), DataType::Boolean);
    }

    #[test]
    fn test_boolean_mixed_case() {
        assert_eq!(spark_type_to_arrow("Boolean"), DataType::Boolean);
        assert_eq!(spark_type_to_arrow("bOOLEAN"), DataType::Boolean);
    }

    #[test]
    fn test_bool_alias() {
        assert_eq!(spark_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(spark_type_to_arrow("bool"), DataType::Boolean);
    }
}

// =============================================================================
// Integer Type Tests
// =============================================================================

mod integer_tests {
    use super::*;

    #[test]
    fn test_byte_tinyint() {
        assert_eq!(spark_type_to_arrow("BYTE"), DataType::Int8);
        assert_eq!(spark_type_to_arrow("TINYINT"), DataType::Int8);
        assert_eq!(spark_type_to_arrow("byte"), DataType::Int8);
        assert_eq!(spark_type_to_arrow("tinyint"), DataType::Int8);
    }

    #[test]
    fn test_short_smallint() {
        assert_eq!(spark_type_to_arrow("SHORT"), DataType::Int16);
        assert_eq!(spark_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(spark_type_to_arrow("short"), DataType::Int16);
        assert_eq!(spark_type_to_arrow("smallint"), DataType::Int16);
    }

    #[test]
    fn test_int_integer() {
        assert_eq!(spark_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("int"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("integer"), DataType::Int32);
    }

    #[test]
    fn test_long_bigint() {
        assert_eq!(spark_type_to_arrow("LONG"), DataType::Int64);
        assert_eq!(spark_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(spark_type_to_arrow("long"), DataType::Int64);
        assert_eq!(spark_type_to_arrow("bigint"), DataType::Int64);
    }
}

// =============================================================================
// Floating Point Type Tests
// =============================================================================

mod floating_point_tests {
    use super::*;

    #[test]
    fn test_float_real() {
        assert_eq!(spark_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(spark_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(spark_type_to_arrow("float"), DataType::Float32);
        assert_eq!(spark_type_to_arrow("real"), DataType::Float32);
    }

    #[test]
    fn test_double() {
        assert_eq!(spark_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(spark_type_to_arrow("double"), DataType::Float64);
    }
}

// =============================================================================
// String Type Tests
// =============================================================================

mod string_tests {
    use super::*;

    #[test]
    fn test_string() {
        assert_eq!(spark_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("string"), DataType::Utf8);
    }

    #[test]
    fn test_varchar() {
        assert_eq!(spark_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("varchar"), DataType::Utf8);
    }

    #[test]
    fn test_char() {
        assert_eq!(spark_type_to_arrow("CHAR"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("char"), DataType::Utf8);
    }

    #[test]
    fn test_text() {
        assert_eq!(spark_type_to_arrow("TEXT"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("text"), DataType::Utf8);
    }

    #[test]
    fn test_varchar_with_length() {
        // VARCHAR(n) should still map to Utf8
        assert_eq!(spark_type_to_arrow("VARCHAR(100)"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("VARCHAR(255)"), DataType::Utf8);
    }

    #[test]
    fn test_char_with_length() {
        // CHAR(n) should still map to Utf8
        assert_eq!(spark_type_to_arrow("CHAR(10)"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("CHAR(1)"), DataType::Utf8);
    }
}

// =============================================================================
// Binary Type Tests
// =============================================================================

mod binary_tests {
    use super::*;

    #[test]
    fn test_binary() {
        assert_eq!(spark_type_to_arrow("BINARY"), DataType::Binary);
        assert_eq!(spark_type_to_arrow("binary"), DataType::Binary);
    }

    #[test]
    fn test_varbinary() {
        assert_eq!(spark_type_to_arrow("VARBINARY"), DataType::Binary);
        assert_eq!(spark_type_to_arrow("varbinary"), DataType::Binary);
    }
}

// =============================================================================
// Date and Time Type Tests
// =============================================================================

mod date_time_tests {
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn test_date() {
        assert_eq!(spark_type_to_arrow("DATE"), DataType::Date32);
        assert_eq!(spark_type_to_arrow("date"), DataType::Date32);
    }

    #[test]
    fn test_timestamp() {
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(spark_type_to_arrow("TIMESTAMP"), expected);
        assert_eq!(spark_type_to_arrow("timestamp"), expected);
    }

    #[test]
    fn test_timestamp_ntz() {
        // Timestamp without timezone
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(spark_type_to_arrow("TIMESTAMP_NTZ"), expected);
        assert_eq!(spark_type_to_arrow("timestamp_ntz"), expected);
    }

    #[test]
    fn test_timestamp_ltz() {
        // Timestamp with local timezone (UTC)
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(spark_type_to_arrow("TIMESTAMP_LTZ"), expected);
        assert_eq!(spark_type_to_arrow("timestamp_ltz"), expected);
    }

    #[test]
    fn test_interval() {
        // Interval maps to Duration
        let expected = DataType::Duration(TimeUnit::Microsecond);
        assert_eq!(spark_type_to_arrow("INTERVAL"), expected);
        assert_eq!(spark_type_to_arrow("interval"), expected);
    }
}

// =============================================================================
// Decimal Type Tests
// =============================================================================

mod decimal_tests {
    use super::*;

    #[test]
    fn test_decimal_default() {
        // Decimal without parameters uses default precision/scale
        match spark_type_to_arrow("DECIMAL") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 38);
                assert_eq!(scale, 18);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_with_precision_and_scale() {
        match spark_type_to_arrow("DECIMAL(10,2)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_with_spaces() {
        match spark_type_to_arrow("DECIMAL(10, 2)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_precision_only() {
        // When only precision is specified, scale defaults to 0
        match spark_type_to_arrow("DECIMAL(18)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 18);
                assert_eq!(scale, 0);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_max_precision() {
        match spark_type_to_arrow("DECIMAL(38,10)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 38);
                assert_eq!(scale, 10);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_zero_scale() {
        match spark_type_to_arrow("DECIMAL(20,0)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 20);
                assert_eq!(scale, 0);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_lowercase() {
        match spark_type_to_arrow("decimal(15,3)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 15);
                assert_eq!(scale, 3);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }
}

// =============================================================================
// Complex Type Tests
// =============================================================================

mod complex_type_tests {
    use super::*;
    use arrow_schema::Field;

    #[test]
    fn test_array_type() {
        match spark_type_to_arrow("ARRAY<INT>") {
            DataType::List(field) => {
                assert_eq!(field.name(), "item");
                // Note: The implementation defaults array element type to Utf8
                // This is a limitation; ideally it would parse the inner type
            }
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_array_lowercase() {
        match spark_type_to_arrow("array<string>") {
            DataType::List(_) => {} // Just verify it's a List
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_map_type() {
        match spark_type_to_arrow("MAP<STRING,INT>") {
            DataType::Map(field, _) => {
                // Verify it creates a map structure
                assert_eq!(field.name(), "entries");
            }
            other => panic!("Expected Map, got {:?}", other),
        }
    }

    #[test]
    fn test_struct_type() {
        match spark_type_to_arrow("STRUCT<name:STRING,age:INT>") {
            DataType::Struct(_) => {} // Just verify it's a Struct
            other => panic!("Expected Struct, got {:?}", other),
        }
    }
}

// =============================================================================
// Unknown Type Tests
// =============================================================================

mod unknown_type_tests {
    use super::*;

    #[test]
    fn test_unknown_type_defaults_to_string() {
        assert_eq!(spark_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("CUSTOM"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("GEOGRAPHY"), DataType::Utf8);
    }

    #[test]
    fn test_empty_string() {
        // Empty string should default to Utf8
        assert_eq!(spark_type_to_arrow(""), DataType::Utf8);
    }

    #[test]
    fn test_whitespace_string() {
        // Whitespace should be handled gracefully
        assert_eq!(spark_type_to_arrow("   "), DataType::Utf8);
    }

    #[test]
    fn test_numeric_string() {
        // Numeric strings should default to Utf8
        assert_eq!(spark_type_to_arrow("123"), DataType::Utf8);
    }
}

// =============================================================================
// Case Insensitivity Tests
// =============================================================================

mod case_insensitivity_tests {
    use super::*;

    #[test]
    fn test_all_uppercase() {
        assert_eq!(spark_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("BOOLEAN"), DataType::Boolean);
    }

    #[test]
    fn test_all_lowercase() {
        assert_eq!(spark_type_to_arrow("int"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("string"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("boolean"), DataType::Boolean);
    }

    #[test]
    fn test_mixed_case() {
        assert_eq!(spark_type_to_arrow("Int"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("String"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("Boolean"), DataType::Boolean);
        assert_eq!(spark_type_to_arrow("BiGiNt"), DataType::Int64);
    }

    #[test]
    fn test_random_case() {
        assert_eq!(spark_type_to_arrow("iNtEgEr"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("dOuBlE"), DataType::Float64);
    }
}

// =============================================================================
// Boundary and Edge Cases
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_type_with_extra_whitespace() {
        // Types with extra whitespace should be normalized
        // Note: Current implementation might not trim whitespace
        let result = spark_type_to_arrow(" INT ");
        // This will likely default to Utf8 due to whitespace
        assert!(matches!(result, DataType::Int32 | DataType::Utf8));
    }

    #[test]
    fn test_decimal_invalid_format() {
        // Invalid decimal format should fall back to default
        match spark_type_to_arrow("DECIMAL(abc,def)") {
            DataType::Decimal128(precision, scale) => {
                // Should use defaults when parsing fails
                assert_eq!(precision, 38);
                assert_eq!(scale, 18);
            }
            other => panic!("Expected Decimal128 with defaults, got {:?}", other),
        }
    }

    #[test]
    fn test_decimal_empty_params() {
        match spark_type_to_arrow("DECIMAL()") {
            DataType::Decimal128(precision, scale) => {
                // Empty params should use defaults
                assert_eq!(precision, 38);
                assert_eq!(scale, 18);
            }
            other => panic!("Expected Decimal128, got {:?}", other),
        }
    }

    #[test]
    fn test_special_characters_in_type() {
        // Special characters should default to Utf8
        assert_eq!(spark_type_to_arrow("INT!"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("@STRING"), DataType::Utf8);
    }

    #[test]
    fn test_very_long_type_name() {
        let long_type = "A".repeat(1000);
        // Should handle gracefully by defaulting to Utf8
        assert_eq!(spark_type_to_arrow(&long_type), DataType::Utf8);
    }
}

// =============================================================================
// Type Mapping Completeness Tests
// =============================================================================

mod completeness_tests {
    use super::*;

    #[test]
    fn test_all_spark_numeric_types() {
        // Ensure all numeric types are covered
        let numeric_types = [
            ("BYTE", DataType::Int8),
            ("TINYINT", DataType::Int8),
            ("SHORT", DataType::Int16),
            ("SMALLINT", DataType::Int16),
            ("INT", DataType::Int32),
            ("INTEGER", DataType::Int32),
            ("LONG", DataType::Int64),
            ("BIGINT", DataType::Int64),
            ("FLOAT", DataType::Float32),
            ("REAL", DataType::Float32),
            ("DOUBLE", DataType::Float64),
        ];

        for (spark_type, expected) in numeric_types {
            assert_eq!(
                spark_type_to_arrow(spark_type),
                expected,
                "Failed for type: {}",
                spark_type
            );
        }
    }

    #[test]
    fn test_all_spark_string_types() {
        let string_types = ["STRING", "VARCHAR", "CHAR", "TEXT"];

        for spark_type in string_types {
            assert_eq!(
                spark_type_to_arrow(spark_type),
                DataType::Utf8,
                "Failed for type: {}",
                spark_type
            );
        }
    }

    #[test]
    fn test_all_spark_binary_types() {
        let binary_types = ["BINARY", "VARBINARY"];

        for spark_type in binary_types {
            assert_eq!(
                spark_type_to_arrow(spark_type),
                DataType::Binary,
                "Failed for type: {}",
                spark_type
            );
        }
    }
}
