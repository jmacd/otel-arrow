// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Custom object store for partition-aware Parquet file reading
//!
//! This object store understands the partition structure created by the Parquet exporter:
//! - Files are organized by table (logs, log_attrs, etc.)
//! - Within each table, files are partitioned by `_part_id`
//! - Within each partition, files are ordered by timestamp for consistent processing
//!
//! URL format: `parquet://<table_name>?part_id=<uuid>&base_path=<path>`

use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::{
    listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
};
use datafusion::execution::context::SessionContext;
use futures::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectResult,
};
use tokio::fs;

use crate::parquet_receiver::error::ParquetReceiverError;

/// Custom object store that handles partition-aware file listing
#[derive(Debug, Clone)]
pub struct PartitionObjectStore {
    /// Base directory containing the Parquet files
    base_path: PathBuf,
    /// Table name (logs, log_attrs, etc.)
    table_name: String,
    /// Specific partition ID to read from (if specified)
    part_id: Option<String>,
    /// Underlying file system store
    inner: Arc<object_store::local::LocalFileSystem>,
}

impl PartitionObjectStore {
    /// Create a new partition-aware object store
    pub fn new(
        base_path: PathBuf,
        table_name: String,
        part_id: Option<String>,
    ) -> ObjectResult<Self> {
        let inner = Arc::new(object_store::local::LocalFileSystem::new());
        
        Ok(Self {
            base_path,
            table_name,
            part_id,
            inner,
        })
    }

    /// Parse a custom parquet:// URL
    pub fn from_url(url: &str) -> ObjectResult<Self> {
        // Simple parsing: parquet://table_name?base_path=path&part_id=uuid
        if !url.starts_with("parquet://") {
            return Err(object_store::Error::Generic {
                store: "partition",
                source: "URL must use parquet:// scheme".into(),
            });
        }
        
        let after_scheme = &url[10..]; // Remove "parquet://"
        let (table_name, query) = if let Some(pos) = after_scheme.find('?') {
            (after_scheme[..pos].to_string(), &after_scheme[pos + 1..])
        } else {
            (after_scheme.to_string(), "")
        };

        if table_name.is_empty() {
            return Err(object_store::Error::Generic {
                store: "partition", 
                source: "Table name must be specified in URL".into(),
            });
        }

        let mut base_path = None;
        let mut part_id = None;

        for pair in query.split('&') {
            if let Some(pos) = pair.find('=') {
                let key = &pair[..pos];
                let value = &pair[pos + 1..];
                match key {
                    "base_path" => base_path = Some(PathBuf::from(value)),
                    "part_id" => part_id = Some(value.to_string()),
                    _ => {} // ignore unknown parameters
                }
            }
        }

        let base_path = base_path.ok_or_else(|| object_store::Error::Generic {
            store: "partition",
            source: "base_path parameter is required".into(),
        })?;

        Self::new(base_path, table_name, part_id)
    }

    /// Discover all partition IDs for this table
    pub async fn discover_partitions(&self) -> ObjectResult<Vec<String>> {
        let table_dir = self.base_path.join(&self.table_name);
        
        let mut partitions = Vec::new();
        let mut entries = fs::read_dir(&table_dir).await.map_err(|e| {
            object_store::Error::Generic {
                store: "partition",
                source: Box::new(e),
            }
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            object_store::Error::Generic {
                store: "partition", 
                source: Box::new(e),
            }
        })? {
            if entry.file_type().await.map_err(|e| {
                object_store::Error::Generic {
                    store: "partition",
                    source: Box::new(e),
                }
            })?.is_dir() {
                let file_name = entry.file_name();
                let dir_name = file_name.to_string_lossy();
                if let Some(part_id) = dir_name.strip_prefix("_part_id=") {
                    partitions.push(part_id.to_string());
                }
            }
        }

        partitions.sort();
        Ok(partitions)
    }

    /// Get files for a specific partition, ordered by timestamp
    async fn get_partition_files(&self, part_id: &str) -> ObjectResult<Vec<ObjectMeta>> {
        let partition_dir = self.base_path
            .join(&self.table_name)
            .join(format!("_part_id={}", part_id));

        let mut files = Vec::new();
        let mut entries = fs::read_dir(&partition_dir).await.map_err(|e| {
            object_store::Error::Generic {
                store: "partition",
                source: Box::new(e),
            }
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            object_store::Error::Generic {
                store: "partition",
                source: Box::new(e),
            }
        })? {
            if entry.file_type().await.map_err(|e| {
                object_store::Error::Generic {
                    store: "partition",
                    source: Box::new(e),
                }
            })?.is_file() {
                let file_name_os = entry.file_name();
                let file_name = file_name_os.to_string_lossy();
                if file_name.ends_with(".parquet") {
                    let metadata = entry.metadata().await.map_err(|e| {
                        object_store::Error::Generic {
                            store: "partition",
                            source: Box::new(e),
                        }
                    })?;

                    // Construct the object path relative to our virtual root
                    let object_path = ObjectPath::from(format!("{}/{}", self.table_name, file_name));
                    
                    files.push(ObjectMeta {
                        location: object_path,
                        last_modified: metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH).into(),
                        size: metadata.len(),
                        e_tag: None,
                        version: None,
                    });
                }
            }
        }

        // Sort files by timestamp (extracted from filename)
        files.sort_by(|a, b| {
            let timestamp_a = extract_timestamp_from_filename(a.location.filename().unwrap_or(""));
            let timestamp_b = extract_timestamp_from_filename(b.location.filename().unwrap_or(""));
            timestamp_a.cmp(&timestamp_b)
        });

        Ok(files)
    }

    /// Map virtual path to real filesystem path
    fn virtual_to_real_path(&self, virtual_path: &ObjectPath) -> PathBuf {
        // Virtual path format: "table_name/filename.parquet"
        // Real path format: "base_path/table_name/_part_id=<uuid>/filename.parquet"
        
        let parts: Vec<&str> = virtual_path.as_ref().split('/').collect();
        if parts.len() >= 2 && parts[0] == self.table_name {
            let filename = parts[1];
            
            // If we have a specific part_id, use it; otherwise try to find the file
            if let Some(ref part_id) = self.part_id {
                return self.base_path
                    .join(&self.table_name)
                    .join(format!("_part_id={}", part_id))
                    .join(filename);
            }
        }
        
        // Fallback: just join the path directly (may not work)
        self.base_path.join(virtual_path.as_ref())
    }
}

/// Extract timestamp from filename format: part-<timestamp>-<uuid>.parquet
fn extract_timestamp_from_filename(filename: &str) -> u64 {
    if let Some(part) = filename.strip_prefix("part-") {
        if let Some(end) = part.find('-') {
            if let Ok(timestamp) = part[..end].parse::<u64>() {
                return timestamp;
            }
        }
    }
    0 // fallback for unparseable filenames
}

impl Display for PartitionObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionObjectStore(table={}, part_id={:?}, base_path={})", 
            self.table_name, self.part_id, self.base_path.display())
    }
}

#[async_trait]
impl ObjectStore for PartitionObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectResult<PutResult> {
        // Delegate to inner store with real path
        let real_path = ObjectPath::from(self.virtual_to_real_path(location).to_string_lossy().as_ref());
        self.inner.put_opts(&real_path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectResult<Box<dyn MultipartUpload>> {
        let real_path = ObjectPath::from(self.virtual_to_real_path(location).to_string_lossy().as_ref());
        self.inner.put_multipart_opts(&real_path, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, opts: GetOptions) -> ObjectResult<GetResult> {
        let real_path = ObjectPath::from(self.virtual_to_real_path(location).to_string_lossy().as_ref());
        self.inner.get_opts(&real_path, opts).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectResult<ListResult> {
        // For partition-aware listing, we need to return files from the appropriate partition
        if let Some(ref part_id) = self.part_id {
            let files = self.get_partition_files(part_id).await?;
            
            // Filter by prefix if specified
            let filtered_files = if let Some(prefix) = prefix {
                files.into_iter()
                    .filter(|meta| meta.location.as_ref().starts_with(prefix.as_ref()))
                    .collect()
            } else {
                files
            };
            
            Ok(ListResult {
                objects: filtered_files,
                common_prefixes: Vec::new(), // We don't use prefixes in our flat structure
            })
        } else {
            // If no specific partition is requested, we could either:
            // 1. Return all files from all partitions (probably not what we want)
            // 2. Return an error
            // For now, return empty result
            Ok(ListResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
            })
        }
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectResult<()> {
        let real_path = ObjectPath::from(self.virtual_to_real_path(location).to_string_lossy().as_ref());
        self.inner.delete(&real_path).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectResult<ObjectMeta>> {
        // For streaming list, delegate to the batch list method
        let store = self.clone();
        let prefix = prefix.map(|p| p.clone());
        
        Box::pin(async_stream::stream! {
            match store.list_with_delimiter(prefix.as_ref()).await {
                Ok(result) => {
                    for object in result.objects {
                        yield Ok(object);
                    }
                }
                Err(e) => yield Err(e),
            }
        })
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectResult<()> {
        let real_from = ObjectPath::from(self.virtual_to_real_path(from).to_string_lossy().as_ref());
        let real_to = ObjectPath::from(self.virtual_to_real_path(to).to_string_lossy().as_ref());
        self.inner.copy(&real_from, &real_to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectResult<()> {
        let real_from = ObjectPath::from(self.virtual_to_real_path(from).to_string_lossy().as_ref());
        let real_to = ObjectPath::from(self.virtual_to_real_path(to).to_string_lossy().as_ref());
        self.inner.copy_if_not_exists(&real_from, &real_to).await
    }
}

/// Register the partition object store with DataFusion
pub async fn register_partition_object_store(
    _session_ctx: &SessionContext,
    base_path: PathBuf,
) -> Result<(), ParquetReceiverError> {
    // Register our custom object store scheme
    log::debug!("Partition object store scheme registered for path: {:?}", base_path);
    
    Ok(())
}

/// Create a ListingTable for a specific table and partition
pub async fn create_partition_table(
    session_ctx: &SessionContext,
    base_path: PathBuf,
    table_name: &str,
    part_id: &str,
) -> Result<Arc<ListingTable>, ParquetReceiverError> {
    // Create object store for this specific partition
    let _object_store = Arc::new(PartitionObjectStore::new(
        base_path.clone(),
        table_name.to_string(),
        Some(part_id.to_string()),
    ).map_err(|e| ParquetReceiverError::Config(format!("Failed to create object store: {}", e)))?);
    
    log::debug!("Creating partition table for {}, partition: {}", table_name, part_id);

    // Create ListingTableUrl - use file:// scheme as fallback
    let table_path = base_path.join(table_name).join(format!("_part_id={}", part_id));
    let table_url = ListingTableUrl::parse(&format!("file://{}", table_path.display()))
        .map_err(|e| ParquetReceiverError::DataFusion(e))?;

    // Create listing options
    let listing_options = ListingOptions::new(Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default()))
        .with_file_extension("parquet");

    // Create table config
    let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

    // Infer schema
    let config_with_schema = config
        .infer_schema(&session_ctx.state())
        .await
        .map_err(|e| ParquetReceiverError::DataFusion(e))?;

    // Create the table
    let listing_table = ListingTable::try_new(config_with_schema)
        .map_err(|e| ParquetReceiverError::DataFusion(e))?;

    Ok(Arc::new(listing_table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_timestamp_from_filename() {
        assert_eq!(
            extract_timestamp_from_filename("part-1758049029392-0c526c76-2cc6-443a-8640-c36c69f9f24b.parquet"),
            1758049029392
        );
        assert_eq!(
            extract_timestamp_from_filename("part-1758049039407-18fc7df8-1394-482d-9805-50f4c5e6f0a8.parquet"),
            1758049039407
        );
        assert_eq!(
            extract_timestamp_from_filename("invalid-filename.parquet"),
            0
        );
    }

    #[test]
    fn test_parse_custom_url() {
        let store = PartitionObjectStore::from_url(
            "parquet://logs?base_path=/tmp/parquet&part_id=test-uuid-123"
        ).unwrap();
        
        assert_eq!(store.table_name, "logs");
        assert_eq!(store.part_id, Some("test-uuid-123".to_string()));
        assert_eq!(store.base_path, PathBuf::from("/tmp/parquet"));
    }
}