/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

// ═══════════════════════════════════════════════════════════════════════════════
// Single cdylib for JDK FFM (Foreign Function & Memory API).
//
// This crate:
//   1. Sets the global jemalloc allocator (shared across all plugin rlibs)
//   2. Pulls in plugin rlibs via extern crate (forces linker to include symbols)
//   3. All #[no_mangle] extern "C" functions from the plugin crates are
//      automatically available for dlsym/SymbolLookup
// ═══════════════════════════════════════════════════════════════════════════════

/// jemalloc init-time options are compiled in via `JEMALLOC_SYS_WITH_MALLOC_CONF`
/// (`--with-malloc-conf`), set by the Gradle `buildRustLibrary` task — see
/// `sandbox/libs/dataformat-native/build.gradle` for the option list and rationale.
///
/// An exported `malloc_conf` static used to live here carrying
/// `dirty_decay_ms`/`muzzy_decay_ms`/`lg_tcache_max:16`. It was removed on the theory that a
/// `_rjem_`-prefixed jemalloc only resolves `_rjem_malloc_conf` and never an unprefixed
/// `malloc_conf` — but removing it made every aggregate query 4-9% slower with identical plans and
/// identical planning time, so it was demonstrably live. Those options now live in the
/// compile-time conf instead (see `build.gradle`), which is the mechanism that is definitely read.
/// Do not add such a static back without also removing them there, or they will be set twice.
///
/// Runtime-tunable options (`dirty_decay_ms`, `muzzy_decay_ms`, `prof_active`, …) are
/// applied through `mallctl` by NativeBridgeModule from cluster settings, which is
/// unaffected by the above. Options that are init-time only — `prof`, `lg_tcache_max`,
/// `thp` — can only come from the compile-time conf.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Pull in plugin rlibs — forces linker to include all #[no_mangle] symbols.
extern crate native_bridge_common;
extern crate opensearch_datafusion;
extern crate opensearch_parquet_format;
extern crate opensearch_repository_azure;
extern crate opensearch_repository_fs;
extern crate opensearch_repository_gcs;
extern crate opensearch_repository_s3;
extern crate opensearch_tiered_storage;
