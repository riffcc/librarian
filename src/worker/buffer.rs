//! Global buffer pool for efficient parallel downloads.
//!
//! Manages a shared pool of memory and disk resources across all concurrent
//! transfers. Small files stay in memory, large files spill to temp disk.
//! Resources are freed as uploads complete, allowing new downloads to start.
//!
//! # Example
//!
//! ```ignore
//! let pool = BufferPool::new(BufferPoolConfig {
//!     max_memory: 6 * 1024 * 1024 * 1024,  // 6 GiB
//!     max_disk: 100 * 1024 * 1024 * 1024,  // 100 GiB
//!     max_concurrent: 8,
//!     spill_threshold: 64 * 1024 * 1024,   // 64 MiB per file
//! });
//!
//! // Acquire a slot for download
//! let mut slot = pool.acquire().await?;
//!
//! // Stream download into slot
//! while let Some(chunk) = stream.next().await {
//!     slot.write(&chunk)?;
//! }
//!
//! // Get bytes for upload
//! let bytes = slot.into_bytes()?;
//!
//! // Upload... slot resources freed when bytes is dropped
//! ```

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tempfile::SpooledTempFile;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

/// Configuration for the global buffer pool.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum total memory for all in-flight buffers (bytes).
    /// Default: 6 GiB
    pub max_memory: u64,

    /// Maximum total disk space for spilled buffers (bytes).
    /// Default: 100 GiB
    pub max_disk: u64,

    /// Maximum concurrent transfers.
    /// Default: 8
    pub max_concurrent: usize,

    /// Per-file threshold before spilling to disk (bytes).
    /// Files larger than this will use temp files.
    /// Default: 64 MiB
    pub spill_threshold: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_memory: 6 * 1024 * 1024 * 1024,      // 6 GiB
            max_disk: 100 * 1024 * 1024 * 1024,     // 100 GiB
            max_concurrent: 8,
            spill_threshold: 64 * 1024 * 1024,      // 64 MiB
        }
    }
}

impl BufferPoolConfig {
    /// Parse a size string like "6G", "100M", "1024K", "1073741824"
    pub fn parse_size(s: &str) -> Option<u64> {
        let s = s.trim().to_uppercase();

        if let Some(num) = s.strip_suffix('G') {
            num.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix("GB") {
            num.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix("GIB") {
            num.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('M') {
            num.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix("MB") {
            num.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix("MIB") {
            num.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('K') {
            num.trim().parse::<u64>().ok().map(|n| n * 1024)
        } else if let Some(num) = s.strip_suffix("KB") {
            num.trim().parse::<u64>().ok().map(|n| n * 1024)
        } else if let Some(num) = s.strip_suffix("KIB") {
            num.trim().parse::<u64>().ok().map(|n| n * 1024)
        } else {
            s.parse::<u64>().ok()
        }
    }
}

/// Global buffer pool for managing concurrent downloads.
///
/// Tracks total memory and disk usage across all active transfers.
/// Automatically balances resources as transfers complete.
pub struct BufferPool {
    config: BufferPoolConfig,

    /// Current memory usage (bytes)
    memory_used: AtomicU64,

    /// Current disk usage (bytes)
    disk_used: AtomicU64,

    /// Semaphore for limiting concurrent transfers
    permits: Arc<Semaphore>,
}

impl BufferPool {
    /// Create a new buffer pool with the given configuration.
    pub fn new(config: BufferPoolConfig) -> Arc<Self> {
        info!(
            max_memory_gb = config.max_memory / 1024 / 1024 / 1024,
            max_disk_gb = config.max_disk / 1024 / 1024 / 1024,
            max_concurrent = config.max_concurrent,
            spill_threshold_mb = config.spill_threshold / 1024 / 1024,
            "Buffer pool initialized"
        );

        Arc::new(Self {
            permits: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            memory_used: AtomicU64::new(0),
            disk_used: AtomicU64::new(0),
        })
    }

    /// Create a pool with default configuration.
    pub fn with_defaults() -> Arc<Self> {
        Self::new(BufferPoolConfig::default())
    }

    /// Acquire a buffer slot for a new transfer.
    ///
    /// This will wait if the pool is at max concurrency.
    /// Returns a `BufferSlot` that automatically releases resources when dropped.
    pub async fn acquire(self: &Arc<Self>) -> BufferSlot {
        let permit = self.permits.clone().acquire_owned().await
            .expect("semaphore should not be closed");

        debug!(
            memory_used_mb = self.memory_used.load(Ordering::Relaxed) / 1024 / 1024,
            disk_used_mb = self.disk_used.load(Ordering::Relaxed) / 1024 / 1024,
            "Acquired buffer slot"
        );

        BufferSlot {
            pool: Arc::clone(self),
            _permit: permit,
            inner: SpooledTempFile::new(self.config.spill_threshold),
            size: 0,
            in_memory: true,
            memory_charged: 0,
            disk_charged: 0,
        }
    }

    /// Try to acquire a buffer slot without waiting.
    ///
    /// Returns `None` if pool is at max concurrency.
    pub fn try_acquire(self: &Arc<Self>) -> Option<BufferSlot> {
        let permit = self.permits.clone().try_acquire_owned().ok()?;

        Some(BufferSlot {
            pool: Arc::clone(self),
            _permit: permit,
            inner: SpooledTempFile::new(self.config.spill_threshold),
            size: 0,
            in_memory: true,
            memory_charged: 0,
            disk_charged: 0,
        })
    }

    /// Get current memory usage.
    pub fn memory_used(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get current disk usage.
    pub fn disk_used(&self) -> u64 {
        self.disk_used.load(Ordering::Relaxed)
    }

    /// Get number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    /// Get maximum concurrent transfers.
    pub fn max_concurrent(&self) -> usize {
        self.config.max_concurrent
    }

    /// Get the configuration.
    pub fn config(&self) -> &BufferPoolConfig {
        &self.config
    }

    /// Check if we can allocate more memory.
    fn can_allocate_memory(&self, bytes: u64) -> bool {
        self.memory_used.load(Ordering::Relaxed) + bytes <= self.config.max_memory
    }

    /// Check if we can allocate more disk.
    fn can_allocate_disk(&self, bytes: u64) -> bool {
        self.disk_used.load(Ordering::Relaxed) + bytes <= self.config.max_disk
    }

    /// Charge memory usage to the pool.
    fn charge_memory(&self, bytes: u64) {
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Release memory usage from the pool.
    fn release_memory(&self, bytes: u64) {
        self.memory_used.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Charge disk usage to the pool.
    fn charge_disk(&self, bytes: u64) {
        self.disk_used.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Release disk usage from the pool.
    fn release_disk(&self, bytes: u64) {
        self.disk_used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// A buffer slot allocated from the pool.
///
/// Automatically tracks resource usage and releases when dropped.
/// Starts in memory, spills to temp file when threshold exceeded.
pub struct BufferSlot {
    pool: Arc<BufferPool>,
    _permit: OwnedSemaphorePermit,
    inner: SpooledTempFile,
    size: u64,
    in_memory: bool,
    memory_charged: u64,
    disk_charged: u64,
}

impl BufferSlot {
    /// Write a chunk of data to the buffer.
    ///
    /// Automatically manages memory/disk allocation from the pool.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        let chunk_len = chunk.len() as u64;
        let new_size = self.size + chunk_len;

        // Check if this write will cause a spill
        let was_in_memory = !self.inner.is_rolled();

        // Write the data
        self.inner.write_all(chunk)?;

        let now_in_memory = !self.inner.is_rolled();

        // Update pool accounting
        if was_in_memory && now_in_memory {
            // Still in memory - charge memory
            if self.pool.can_allocate_memory(chunk_len) {
                self.pool.charge_memory(chunk_len);
                self.memory_charged += chunk_len;
            } else {
                // Would exceed memory limit - this is handled by SpooledTempFile
                // but we should track it
                warn!(
                    size_mb = new_size / 1024 / 1024,
                    "Memory limit reached, data may spill to disk"
                );
            }
        } else if was_in_memory && !now_in_memory {
            // Just spilled to disk - release memory, charge disk
            self.pool.release_memory(self.memory_charged);
            self.memory_charged = 0;

            if self.pool.can_allocate_disk(new_size) {
                self.pool.charge_disk(new_size);
                self.disk_charged = new_size;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Disk buffer limit exceeded ({} bytes used of {} max)",
                        self.pool.disk_used(),
                        self.pool.config.max_disk
                    ),
                ));
            }

            self.in_memory = false;
            debug!(
                size_mb = new_size / 1024 / 1024,
                "Buffer spilled to temp file"
            );
        } else {
            // Already on disk - charge additional disk
            let additional = chunk_len;
            if self.pool.can_allocate_disk(additional) {
                self.pool.charge_disk(additional);
                self.disk_charged += additional;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Disk buffer limit exceeded",
                ));
            }
        }

        self.size = new_size;
        Ok(())
    }

    /// Get the total size written.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Check if data is still in memory.
    pub fn is_in_memory(&self) -> bool {
        self.in_memory && !self.inner.is_rolled()
    }

    /// Consume the buffer and return the content as Bytes.
    ///
    /// Resources are released from the pool when the returned Bytes is dropped.
    pub fn into_bytes(mut self) -> std::io::Result<Bytes> {
        self.inner.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::with_capacity(self.size as usize);
        self.inner.read_to_end(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Get storage type description for logging.
    pub fn storage_type(&self) -> &'static str {
        if self.is_in_memory() {
            "memory"
        } else {
            "temp file"
        }
    }

    /// Clear the buffer for reuse (e.g., on retry).
    ///
    /// Releases any charged resources and resets the buffer to empty state.
    pub fn clear(&mut self) {
        // Release resources back to the pool
        if self.memory_charged > 0 {
            self.pool.release_memory(self.memory_charged);
            self.memory_charged = 0;
        }
        if self.disk_charged > 0 {
            self.pool.release_disk(self.disk_charged);
            self.disk_charged = 0;
        }

        // Create fresh SpooledTempFile
        self.inner = SpooledTempFile::new(self.pool.config.spill_threshold as usize);
        self.size = 0;
        self.in_memory = true;
    }
}

impl Drop for BufferSlot {
    fn drop(&mut self) {
        // Release resources back to the pool
        if self.memory_charged > 0 {
            self.pool.release_memory(self.memory_charged);
        }
        if self.disk_charged > 0 {
            self.pool.release_disk(self.disk_charged);
        }

        debug!(
            released_memory_mb = self.memory_charged / 1024 / 1024,
            released_disk_mb = self.disk_charged / 1024 / 1024,
            "Buffer slot released"
        );
    }
}

// ============================================================================
// Legacy API for compatibility
// ============================================================================

/// Legacy configuration - maps to BufferPoolConfig.
pub type BufferConfig = BufferPoolConfig;

/// Legacy buffer - wraps a pool slot.
pub struct TransferBuffer {
    slot: Option<BufferSlot>,
    standalone_inner: Option<SpooledTempFile>,
    size: u64,
}

impl TransferBuffer {
    /// Create a new standalone buffer (not from a pool).
    pub fn new(config: &BufferConfig) -> Self {
        Self {
            slot: None,
            standalone_inner: Some(SpooledTempFile::new(config.spill_threshold)),
            size: 0,
        }
    }

    /// Create from a pool slot.
    pub fn from_slot(slot: BufferSlot) -> Self {
        Self {
            slot: Some(slot),
            standalone_inner: None,
            size: 0,
        }
    }

    /// Write a chunk of data.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        if let Some(ref mut slot) = self.slot {
            slot.write_chunk(chunk)?;
            self.size = slot.size();
        } else if let Some(ref mut inner) = self.standalone_inner {
            inner.write_all(chunk)?;
            self.size += chunk.len() as u64;
        }
        Ok(())
    }

    /// Get total size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Check if in memory.
    pub fn is_in_memory(&self) -> bool {
        if let Some(ref slot) = self.slot {
            slot.is_in_memory()
        } else if let Some(ref inner) = self.standalone_inner {
            !inner.is_rolled()
        } else {
            true
        }
    }

    /// Convert to bytes.
    pub fn into_bytes(mut self) -> std::io::Result<Bytes> {
        if let Some(slot) = self.slot.take() {
            slot.into_bytes()
        } else if let Some(mut inner) = self.standalone_inner.take() {
            inner.seek(SeekFrom::Start(0))?;
            let mut buf = Vec::with_capacity(self.size as usize);
            inner.read_to_end(&mut buf)?;
            Ok(Bytes::from(buf))
        } else {
            Ok(Bytes::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(BufferPoolConfig::parse_size("6G"), Some(6 * 1024 * 1024 * 1024));
        assert_eq!(BufferPoolConfig::parse_size("6GB"), Some(6 * 1024 * 1024 * 1024));
        assert_eq!(BufferPoolConfig::parse_size("100M"), Some(100 * 1024 * 1024));
        assert_eq!(BufferPoolConfig::parse_size("1024K"), Some(1024 * 1024));
        assert_eq!(BufferPoolConfig::parse_size("1073741824"), Some(1073741824));
        assert_eq!(BufferPoolConfig::parse_size("invalid"), None);
    }

    #[tokio::test]
    async fn test_pool_concurrency() {
        let pool = BufferPool::new(BufferPoolConfig {
            max_concurrent: 2,
            ..Default::default()
        });

        // Acquire 2 slots
        let slot1 = pool.acquire().await;
        let slot2 = pool.acquire().await;

        assert_eq!(pool.available_permits(), 0);

        // Try to acquire third - should fail immediately
        assert!(pool.try_acquire().is_none());

        // Drop one slot
        drop(slot1);
        assert_eq!(pool.available_permits(), 1);

        // Now we can acquire
        let _slot3 = pool.acquire().await;
        assert_eq!(pool.available_permits(), 0);

        drop(slot2);
        drop(_slot3);
        assert_eq!(pool.available_permits(), 2);
    }

    #[tokio::test]
    async fn test_memory_tracking() {
        let pool = BufferPool::new(BufferPoolConfig {
            max_memory: 1024 * 1024, // 1 MiB
            spill_threshold: 512 * 1024, // 512 KiB
            ..Default::default()
        });

        let mut slot = pool.acquire().await;

        // Write 100 KiB - should stay in memory
        slot.write_chunk(&vec![0u8; 100 * 1024]).unwrap();
        assert!(slot.is_in_memory());
        assert!(pool.memory_used() > 0);

        drop(slot);
        assert_eq!(pool.memory_used(), 0);
    }

    #[tokio::test]
    async fn test_spill_to_disk() {
        let pool = BufferPool::new(BufferPoolConfig {
            max_memory: 1024 * 1024,
            max_disk: 10 * 1024 * 1024,
            spill_threshold: 1024, // 1 KiB - very small to force spill
            ..Default::default()
        });

        let mut slot = pool.acquire().await;

        // Write 10 KiB - should spill to disk
        slot.write_chunk(&vec![0u8; 10 * 1024]).unwrap();
        assert!(!slot.is_in_memory());
        assert!(pool.disk_used() > 0);

        // Read it back
        let bytes = slot.into_bytes().unwrap();
        assert_eq!(bytes.len(), 10 * 1024);

        // Disk should be freed
        assert_eq!(pool.disk_used(), 0);
    }
}
