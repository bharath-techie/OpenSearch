/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`LatencyStore`] — an [`ObjectStore`] decorator that sleeps for a configurable
//! duration before every read, to model a high-latency object store (real S3,
//! cross-AZ) on a benchmark node whose data is actually warm-cache local.
//!
//! # Why this exists on `main`
//!
//! The IO-runtime work lives on a feature branch (`SpawnIoStore`); `main` has no
//! IO runtime to dispatch onto. But the *latency* lever is independent of where
//! reads run: injecting per-read latency on `main` establishes the **baseline**
//! — how the current (no-isolation) read path behaves as IO latency grows —
//! against which the branch's pool-isolation result is compared. Without a
//! baseline at the same injected latencies, the branch numbers have nothing to
//! be measured against.
//!
//! # How
//!
//! The sleep runs *inline* on whatever runtime drives the read (on `main` that
//! is the CPU/query runtime — there is no separate IO pool). That is exactly the
//! point: it shows the cost of IO latency when reads are **not** isolated, i.e.
//! a slow fetch stalls the thread that would otherwise be decoding.
//!
//! Wrapping the *store* (rather than individual readers) means every reader —
//! the indexed reader, DataFusion's stock `ParquetObjectReader` on the
//! `ListingTable` path, statistics/metadata probes — sees the injected latency,
//! because they all go through the registered [`ObjectStore`]. Reads
//! (`get_opts`/`get_ranges`) are delayed; writes/list/delete delegate unchanged.
//!
//! Latency is read once from `DATAFUSION_IO_INJECT_LATENCY_MS` (whole ms; absent
//! / unparseable / `0` ⇒ disabled). [`LatencyStore::wrap`] is a no-op passthrough
//! when disabled, so the normal benchmark pays nothing.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use prost::bytes::Bytes;

/// Artificial per-read latency, read once from `DATAFUSION_IO_INJECT_LATENCY_MS`.
///
/// The `OnceLock` means the env lookup happens at most once per process, so when
/// disabled the check is a single relaxed load with no per-read cost.
fn injected_latency() -> Option<Duration> {
    static LATENCY: OnceLock<Option<Duration>> = OnceLock::new();
    *LATENCY.get_or_init(|| {
        std::env::var("DATAFUSION_IO_INJECT_LATENCY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(Duration::from_millis)
    })
}

/// Wraps an inner [`ObjectStore`] so that every read sleeps for the injected
/// latency before delegating.
#[derive(Debug)]
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
}

impl LatencyStore {
    /// Wrap `inner` with the latency configured by `DATAFUSION_IO_INJECT_LATENCY_MS`.
    /// Returns `inner` unwrapped when injection is disabled (the default), so the
    /// store hierarchy and behaviour are unchanged in normal runs.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        match injected_latency() {
            Some(delay) => Arc::new(LatencyStore { inner, delay }),
            None => inner,
        }
    }
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStore({}, {:?})", self.inner, self.delay)
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    // ── Reads: delayed by the injected latency ───────────────────────────────

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        tokio::time::sleep(self.delay).await;
        self.inner.get_opts(location, options).await
    }

    // `get_range` is a provided method on `ObjectStoreExt` that funnels through
    // `get_opts`, so it inherits the delay automatically — nothing to override.

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        tokio::time::sleep(self.delay).await;
        self.inner.get_ranges(location, ranges).await
    }

    // ── Everything else: delegate unchanged ──────────────────────────────────

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
