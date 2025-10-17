use crate::{
    error::{WalError, WalResult},
    wal_event::{ColumnInfo, TupleValue},
};
use std::str;

/// Parse tuple data into structured values
pub struct TupleParser;

impl TupleParser {
    /// Parse raw tuple bytes into a vector of values
    /// The bytes format is: value1\0value2\0value3\0 or 255 for NULL
    pub fn parse_tuple_bytes(
        raw_bytes: &[u8],
        columns: &[ColumnInfo],
    ) -> WalResult<Vec<(String, TupleValue)>> {
        let mut result = Vec::with_capacity(columns.len());
        let mut current_pos = 0;
        
        for col_info in columns {
            if current_pos >= raw_bytes.len() {
                return Err(WalError::DecodingError(format!(
                    "Unexpected end of tuple data at column '{}'",
                    col_info.name
                )));
            }

            // Check for NULL marker (255)
            if raw_bytes[current_pos] == 255 {
                result.push((col_info.name.clone(), TupleValue::Null));
                current_pos += 1;
                continue;
            }

            // Find the next delimiter (0)
            let end_pos = raw_bytes[current_pos..]
                .iter()
                .position(|&b| b == 0)
                .map(|p| current_pos + p)
                .unwrap_or(raw_bytes.len());

            let value_bytes = &raw_bytes[current_pos..end_pos];
            
            let value = if value_bytes.is_empty() {
                TupleValue::Null
            } else {
                // Try to parse as text first
                match str::from_utf8(value_bytes) {
                    Ok(text) => TupleValue::Text(text.to_string()),
                    Err(_) => TupleValue::Binary(value_bytes.to_vec()),
                }
            };

            result.push((col_info.name.clone(), value));
            current_pos = end_pos + 1; // Skip past the delimiter
        }

        Ok(result)
    }

    /// Extract a specific column value by name
    pub fn get_column_value<'a>(
        parsed: &'a[(String, TupleValue)],
        column_name: &str,
    ) -> Option<&'a TupleValue> {
        parsed
            .iter()
            .find(|(name, _)| name == column_name)
            .map(|(_, value)| value)
    }

    /// Extract text value from a column
    pub fn get_text_value(
        parsed: &[(String, TupleValue)],
        column_name: &str,
    ) -> WalResult<String> {
        match Self::get_column_value(parsed, column_name) {
            Some(TupleValue::Text(s)) => Ok(s.clone()),
            Some(TupleValue::Null) => Err(WalError::DecodingError(format!(
                "Column '{}' is NULL",
                column_name
            ))),
            Some(TupleValue::Binary(_)) => Err(WalError::DecodingError(format!(
                "Column '{}' is binary data",
                column_name
            ))),
            None => Err(WalError::DecodingError(format!(
                "Column '{}' not found",
                column_name
            ))),
        }
    }

    /// Parse an integer value from a column
    pub fn parse_i32(
        parsed: &[(String, TupleValue)],
        column_name: &str,
    ) -> WalResult<i32> {
        let text = Self::get_text_value(parsed, column_name)?;
        text.parse::<i32>().map_err(|e| {
            WalError::DecodingError(format!(
                "Failed to parse '{}' as i32: {}",
                column_name, e
            ))
        })
    }

    /// Parse a long integer value from a column
    pub fn parse_i64(
        parsed: &[(String, TupleValue)],
        column_name: &str,
    ) -> WalResult<i64> {
        let text = Self::get_text_value(parsed, column_name)?;
        text.parse::<i64>().map_err(|e| {
            WalError::DecodingError(format!(
                "Failed to parse '{}' as i64: {}",
                column_name, e
            ))
        })
    }

    /// Parse a boolean value from a column
    pub fn parse_bool(
        parsed: &[(String, TupleValue)],
        column_name: &str,
    ) -> WalResult<bool> {
        let text = Self::get_text_value(parsed, column_name)?;
        match text.as_str() {
            "t" | "true" | "1" => Ok(true),
            "f" | "false" | "0" => Ok(false),
            _ => Err(WalError::DecodingError(format!(
                "Failed to parse '{}' as bool: '{}'",
                column_name, text
            ))),
        }
    }
}