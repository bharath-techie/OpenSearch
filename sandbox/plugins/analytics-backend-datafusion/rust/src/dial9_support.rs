/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! dial9 tokio-telemetry integration (Linux-only, feature `dial9`).
//!
//! dial9 records per-poll/park/wake tokio events + CPU flamegraphs + schedule
//! profiling into a self-describing trace file, viewed in a browser via
//! `dial9 serve`. It instruments a runtime by *building* it through a shared
//! [`TelemetryCore`] guard rather than `tokio::runtime::Builder::build()`.
//!
//! Our two runtimes (IO in `runtime_manager`, CPU in `executor::DedicatedExecutor`)
//! are built in different places — the CPU one inside a driver thread — so the
//! guard lives in a process-global `OnceLock`. Both call sites funnel through
//! [`build_runtime`], which:
//!   * with the guard present: `guard.trace_runtime(name).with_tokio_hooks(..).build(builder)`
//!     — dial9's hooks run first, then our `on_thread_start` (so `register_io_runtime`
//!     still fires on every worker; dial9 explicitly composes rather than clobbers).
//!   * without the guard / on non-Linux / feature off: a plain
//!     `builder.on_thread_start(on_start).build()`, identical to the original code.
//!
//! Trace dir: `DIAL9_TRACE_DIR` (default `/tmp/dial9-traces`). Enabled only when
//! `DIAL9_ENABLED=1` so a normal `--features dial9` build still runs untouched
//! unless you opt in at runtime.

use tokio::runtime::{Builder, Runtime};

#[cfg(all(feature = "dial9", target_os = "linux"))]
mod imp {
    use super::*;
    use dial9_tokio_telemetry::telemetry::cpu_profile::{CpuProfilingConfig, SchedEventConfig};
    use dial9_tokio_telemetry::telemetry::{RotatingWriter, TokioHooks, TracedRuntime};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    // dial9 0.3.13 exposes CPU profiling only on the SINGLE-runtime `TracedRuntime`
    // builder (not the multi-runtime `TelemetryCore`). Since the interesting work
    // (long CPU polls) is on the CPU runtime, we trace ONLY that one with CPU +
    // sched profiling + tokio events; the IO runtime stays plain. The resulting
    // TelemetryGuard is stashed process-global so shutdown() can flush it.
    type Guard = dial9_tokio_telemetry::telemetry::TelemetryGuard;
    static GUARD: OnceLock<Mutex<Option<Guard>>> = OnceLock::new();

    fn trace_dir() -> String {
        std::env::var("DIAL9_TRACE_DIR").unwrap_or_else(|_| "/tmp/dial9-traces".to_string())
    }

    fn enabled() -> bool {
        matches!(std::env::var("DIAL9_ENABLED").as_deref(), Ok("1") | Ok("true"))
    }

    /// No-op now: the traced runtime is constructed lazily in `build_runtime("cpu")`.
    /// Kept so call sites don't need cfg-gating. Logs intent.
    pub fn init() {
        if enabled() {
            let _ = std::fs::create_dir_all(trace_dir());
            log::info!("dial9: enabled — CPU runtime will be traced, dir = {}", trace_dir());
        } else {
            log::info!("dial9: DIAL9_ENABLED not set — telemetry off (plain runtimes)");
        }
    }

    /// Build `builder` as a traced runtime when this is the "cpu" runtime and
    /// telemetry is enabled; otherwise a plain runtime. `on_start` always runs on
    /// every worker thread start (dial9 composes user hooks AFTER its own).
    pub fn build_runtime<F>(name: &str, mut builder: Builder, on_start: F) -> Runtime
    where
        F: Fn() + Send + Sync + 'static,
    {
        if enabled() && name == "cpu" {
            let base = format!("{}/trace.bin", trace_dir().trim_end_matches('/'));
            // RotatingWriter::new(base_path, max_file_size, max_total_size).
            let writer = match RotatingWriter::new(&base, 100 * 1024 * 1024, 4 * 1024 * 1024 * 1024) {
                Ok(w) => w,
                Err(e) => {
                    log::warn!("dial9: RotatingWriter failed: {e} — plain cpu runtime");
                    return builder.on_thread_start(on_start).build().expect("cpu runtime");
                }
            };
            let add_hooks = move |h: &mut TokioHooks| {
                h.on_thread_start(on_start);
            };
            match TracedRuntime::builder()
                .with_cpu_profiling(CpuProfilingConfig::default())
                .with_sched_events(SchedEventConfig::default())
                .with_tokio_hooks(add_hooks)
                .with_trace_path(&base)
                .build_and_start(builder, writer)
            {
                Ok((rt, guard)) => {
                    let _ = GUARD.set(Mutex::new(Some(guard)));
                    log::info!("dial9: cpu runtime traced (CPU + sched + tokio events), trace = {base}");
                    return rt;
                }
                Err(e) => {
                    panic!("dial9: TracedRuntime build_and_start failed: {e}");
                }
            }
        }
        builder.on_thread_start(on_start).build().expect("Creating tokio runtime")
    }

    /// Flush + stop telemetry (consumes the guard). Call on shutdown.
    pub fn shutdown() {
        if let Some(cell) = GUARD.get() {
            if let Ok(mut g) = cell.lock() {
                if let Some(guard) = g.take() {
                    let _ = guard.graceful_shutdown(Duration::from_secs(5));
                    log::info!("dial9: telemetry flushed and shut down");
                }
            }
        }
    }
}

// ---- public API: thin shims that compile on every platform/feature combo ----

/// Initialize dial9 telemetry (no-op unless built `--features dial9` on Linux
/// AND `DIAL9_ENABLED=1` at runtime).
pub fn init() {
    #[cfg(all(feature = "dial9", target_os = "linux"))]
    imp::init();
}

/// Build a (possibly traced) multi-thread runtime. `on_start` runs on every
/// worker thread start. Outside the dial9 feature this is just
/// `builder.on_thread_start(on_start).build()`.
pub fn build_runtime<F>(_name: &str, mut builder: Builder, on_start: F) -> Runtime
where
    F: Fn() + Send + Sync + 'static,
{
    #[cfg(all(feature = "dial9", target_os = "linux"))]
    {
        return imp::build_runtime(_name, builder, on_start);
    }
    #[cfg(not(all(feature = "dial9", target_os = "linux")))]
    {
        builder.on_thread_start(on_start).build().expect("Creating tokio runtime")
    }
}

/// Flush + stop dial9 telemetry (no-op unless active).
pub fn shutdown() {
    #[cfg(all(feature = "dial9", target_os = "linux"))]
    imp::shutdown();
}
