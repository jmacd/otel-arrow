// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! File discovery for parquet files
//! 
//! This module handles discovering new parquet files in the directory structure
//! and extracting metadata like partition IDs and timestamps.

use crate::parquet_receiver::{config::SignalType, error::ParquetReceiverError};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Information about discovered parquet files
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredFile {
    /// Path to the main signal file (e.g., logs/part-...parquet)
    pub path: PathBuf,
    /// Extracted partition ID from the directory structure
    pub partition_id: Uuid,
    /// Signal type (logs, traces, metrics)
    pub signal_type: SignalType,
    /// File modification time for age checking
    pub modified_time: SystemTime,
    /// Expected related files for this signal
    pub related_files: Vec<RelatedFile>,
}

/// Information about related attribute files
#[derive(Debug, Clone, PartialEq)]
pub struct RelatedFile {
    /// Path to the related file
    pub path: PathBuf,
    /// Type of related file (log_attrs, resource_attrs, etc.)
    pub file_type: String,
}

/// File discovery engine
pub struct FileDiscovery {
    base_path: PathBuf,
    processed_files: HashSet<PathBuf>,
    min_file_age: Option<Duration>,
}

impl FileDiscovery {
    /// Create a new file discovery instance
    pub fn new(base_path: PathBuf, min_file_age: Option<Duration>) -> Self {
        Self {
            base_path,
            processed_files: HashSet::new(),
            min_file_age,
        }
    }

    /// Discover new parquet files for the given signal types
    pub fn discover_new_files(
        &mut self,
        signal_types: &[SignalType],
    ) -> Result<Vec<DiscoveredFile>, ParquetReceiverError> {
        log::debug!("🔍 FileDiscovery::discover_new_files called for signal types: {:?}", signal_types);
        log::debug!("🔍 Base path: {}", self.base_path.display());
        log::debug!("🔍 Currently processed files count: {}", self.processed_files.len());
        
        let mut discovered = Vec::new();

        for signal_type in signal_types {
            let signal_dir = self.base_path.join(signal_type_to_dir_name(signal_type));
            log::debug!("🔍 Looking for {:?} files in directory: {}", signal_type, signal_dir.display());
            
            if !signal_dir.exists() {
                log::debug!("⚠️ Directory does not exist: {}", signal_dir.display());
                continue;
            }

            log::debug!("✅ Directory exists, scanning for files...");
            let files = self.discover_signal_files(&signal_dir, signal_type)?;
            log::debug!("📁 Found {} files for {:?}", files.len(), signal_type);
            discovered.extend(files);
        }

        // Sort by modification time to process files in order
        discovered.sort_by_key(|f| f.modified_time);
        log::debug!("📋 Total discovered files: {}", discovered.len());
        
        for file in &discovered {
            log::debug!("📄 Discovered: {}", file.path.display());
        }

        Ok(discovered)
    }

    /// Discover files for a specific signal type
    fn discover_signal_files(
        &mut self,
        signal_dir: &Path,
        signal_type: &SignalType,
    ) -> Result<Vec<DiscoveredFile>, ParquetReceiverError> {
        let mut discovered = Vec::new();

        // Look for partition directories (_part_id=<uuid>)
        for entry in fs::read_dir(signal_dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            let dir_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    ParquetReceiverError::FileDiscovery(format!(
                        "Invalid directory name: {:?}",
                        path
                    ))
                })?;

            if !dir_name.starts_with("_part_id=") {
                continue;
            }

            let partition_id = extract_partition_id(dir_name)?;
            let files = self.discover_partition_files(&path, partition_id, signal_type)?;
            discovered.extend(files);
        }

        Ok(discovered)
    }

    /// Discover parquet files within a partition directory
    fn discover_partition_files(
        &mut self,
        partition_dir: &Path,
        partition_id: Uuid,
        signal_type: &SignalType,
    ) -> Result<Vec<DiscoveredFile>, ParquetReceiverError> {
        let mut discovered = Vec::new();

        for entry in fs::read_dir(partition_dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() || !path.extension().map_or(false, |ext| ext == "parquet") {
                continue;
            }

            // Skip if already processed
            if self.processed_files.contains(&path) {
                continue;
            }

            // Check file age if required
            if let Some(min_age) = self.min_file_age {
                let metadata = fs::metadata(&path)?;
                let modified = metadata.modified()?;
                let age = SystemTime::now().duration_since(modified).unwrap_or_default();
                if age < min_age {
                    continue;
                }
            }

            let metadata = fs::metadata(&path)?;
            let modified_time = metadata.modified()?;

            // Find related files
            let related_files = self.find_related_files(partition_id, signal_type)?;

            let discovered_file = DiscoveredFile {
                path: path.clone(),
                partition_id,
                signal_type: signal_type.clone(),
                modified_time,
                related_files,
            };

            discovered.push(discovered_file);
            let _ = self.processed_files.insert(path);
        }

        Ok(discovered)
    }

    /// Find related attribute files for a partition and signal type
    fn find_related_files(
        &self,
        partition_id: Uuid,
        signal_type: &SignalType,
    ) -> Result<Vec<RelatedFile>, ParquetReceiverError> {
        let mut related = Vec::new();
        let related_types = get_related_file_types(signal_type);

        for file_type in related_types {
            let related_dir = self
                .base_path
                .join(file_type)
                .join(format!("_part_id={}", partition_id));

            if related_dir.exists() {
                // Find parquet files in the related directory
                for entry in fs::read_dir(&related_dir)? {
                    let entry = entry?;
                    let path = entry.path();

                    if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
                        related.push(RelatedFile {
                            path,
                            file_type: file_type.to_string(),
                        });
                    }
                }
            }
        }

        Ok(related)
    }

    /// Mark a file as processed
    pub fn mark_processed(&mut self, file_path: &Path) {
        let _ = self.processed_files.insert(file_path.to_path_buf());
    }

    /// Get count of processed files (for testing/monitoring)
    pub fn processed_count(&self) -> usize {
        self.processed_files.len()
    }
}

/// Extract partition ID from directory name like "_part_id=550e8400-e29b-41d4-a716-446655440000"
fn extract_partition_id(dir_name: &str) -> Result<Uuid, ParquetReceiverError> {
    let uuid_str = dir_name
        .strip_prefix("_part_id=")
        .ok_or_else(|| ParquetReceiverError::InvalidPartition(format!("Invalid partition directory name: {}", dir_name)))?;

    Uuid::parse_str(uuid_str).map_err(|e| {
        ParquetReceiverError::InvalidPartition(format!("Invalid UUID in partition name: {}", e))
    })
}

/// Convert signal type to directory name
fn signal_type_to_dir_name(signal_type: &SignalType) -> &'static str {
    match signal_type {
        SignalType::Logs => "logs",
        SignalType::Traces => "spans",
        SignalType::Metrics => "univariate_metrics",
    }
}

/// Get related file types for a signal type
fn get_related_file_types(signal_type: &SignalType) -> Vec<&'static str> {
    match signal_type {
        SignalType::Logs => vec!["log_attrs", "resource_attrs", "scope_attrs"],
        SignalType::Traces => vec![
            "span_attrs",
            "span_events",
            "span_event_attrs",
            "span_links",
            "span_link_attrs",
            "resource_attrs",
            "scope_attrs",
        ],
        SignalType::Metrics => vec![
            "number_data_points",
            "number_dp_attrs",
            "histogram_data_points",
            "histogram_dp_attrs",
            "resource_attrs",
            "scope_attrs",
            // Note: simplified for hackathon - full list would include all metric types
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_test_directory() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        // Create test directory structure
        let partition_id = Uuid::new_v4();
        let logs_partition_dir = base_path.join("logs").join(format!("_part_id={}", partition_id));
        fs::create_dir_all(&logs_partition_dir).unwrap();

        // Create a test parquet file
        let test_file = logs_partition_dir.join("part-001.parquet");
        fs::write(&test_file, b"fake parquet data").unwrap();

        // Create related attribute files
        let log_attrs_dir = base_path
            .join("log_attrs")
            .join(format!("_part_id={}", partition_id));
        fs::create_dir_all(&log_attrs_dir).unwrap();
        fs::write(log_attrs_dir.join("part-001.parquet"), b"fake attrs").unwrap();

        (temp_dir, base_path)
    }

    #[test]
    fn test_file_discovery() {
        let (_temp_dir, base_path) = setup_test_directory();
        let mut discovery = FileDiscovery::new(base_path, None);

        let discovered = discovery
            .discover_new_files(&[SignalType::Logs])
            .unwrap();

        assert_eq!(discovered.len(), 1);
        let file = &discovered[0];
        assert_eq!(file.signal_type, SignalType::Logs);
        assert!(!file.related_files.is_empty());
    }

    #[test]
    fn test_extract_partition_id() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let dir_name = format!("_part_id={}", uuid_str);
        let uuid = extract_partition_id(&dir_name).unwrap();
        assert_eq!(uuid.to_string(), uuid_str);
    }

    #[test]
    fn test_processed_files_tracking() {
        let (_temp_dir, base_path) = setup_test_directory();
        let mut discovery = FileDiscovery::new(base_path, None);

        // First discovery should find files
        let discovered1 = discovery
            .discover_new_files(&[SignalType::Logs])
            .unwrap();
        assert_eq!(discovered1.len(), 1);

        // Second discovery should find no new files
        let discovered2 = discovery
            .discover_new_files(&[SignalType::Logs])
            .unwrap();
        assert_eq!(discovered2.len(), 0);
    }
}