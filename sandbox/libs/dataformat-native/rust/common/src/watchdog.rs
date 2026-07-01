/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stuck-operation watchdog for detecting PERMANENT wedges.
//!
//! The old `-slow`/`-stall` markers computed `elapsed` *after* the blocking call
//! returned, so a permanently-wedged call (the exact thing we hunt) never returned
//! and never logged. This watchdog fires from an independent OS thread while the
//! operation is STILL blocked.
//!
//! Design constraints:
//! - **No tokio.** The watchdog thread is a plain `std::thread` so it keeps ticking
//!   even when every tokio runtime worker is saturated or parked — otherwise it would
//!   wedge alongside the thing it is trying to observe.
//! - **Cheap register/deregister.** One mutex lock + hashmap insert/remove per guarded
//!   op. Guarded ops are FFM-boundary calls (`df_stream_next`, `df_execute_query`, gate
//!   acquire), which are already coarse-grained, so this is negligible.
//! - **RAII.** [`WatchGuard`] deregisters on drop, so normal completion is silent and
//!   only genuinely stuck ops are reported.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::logger::{self, LogLevel};

/// An operation currently in flight, tracked by the watchdog.
struct InFlight {
    /// Human label, e.g. `stream_next stream_ptr=0x..`.
    label: String,
    /// When the op started.
    start: Instant,
    /// Report as stuck once in-flight longer than this.
    threshold: Duration,
    /// Last multiple-of-threshold at which we already logged, so we re-warn
    /// periodically (every `threshold`) instead of once or every tick.
    last_reported_secs: u64,
}

struct Registry {
    ops: Mutex<HashMap<u64, InFlight>>,
    next_id: AtomicU64,
    started: AtomicBool,
}

fn registry() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| Registry {
        ops: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(1),
        started: AtomicBool::new(false),
    })
}

/// Start the watchdog thread once. Idempotent. Safe to call from any FFM entry point.
fn ensure_started() {
    let reg = registry();
    // Cheap fast-path: already running.
    if reg.started.load(Ordering::Relaxed) {
        return;
    }
    // Only the thread that flips false->true spawns the ticker.
    if reg
        .started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    std::thread::Builder::new()
        .name("df-watchdog".to_string())
        .spawn(|| watchdog_loop())
        .ok();
}

fn watchdog_loop() {
    let reg = registry();
    loop {
        std::thread::sleep(Duration::from_secs(1));
        // Error-level so it survives the default INFO gate; the whole point is to be
        // visible exactly when everything else has gone quiet.
        if !logger::enabled(LogLevel::Error) {
            continue;
        }
        let mut guard = match reg.ops.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for inflight in guard.values_mut() {
            let elapsed = inflight.start.elapsed();
            if elapsed < inflight.threshold {
                continue;
            }
            // Re-report every `threshold` seconds while still stuck.
            let bucket = elapsed.as_secs() / inflight.threshold.as_secs().max(1);
            if bucket <= inflight.last_reported_secs {
                continue;
            }
            inflight.last_reported_secs = bucket;
            logger::log(
                LogLevel::Error,
                &format!(
                    "[WATCHDOG-STUCK] {} still blocked after {:?} — operation has NOT returned (permanent wedge suspected)",
                    inflight.label, elapsed
                ),
            );
        }
    }
}

/// RAII guard: registers an in-flight op on creation, deregisters on drop.
#[must_use]
pub struct WatchGuard {
    id: u64,
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        let reg = registry();
        if let Ok(mut ops) = reg.ops.lock() {
            ops.remove(&self.id);
        }
    }
}

/// Begin watching an operation. If it has not been dropped within `threshold`,
/// the watchdog thread logs `[WATCHDOG-STUCK]` and re-logs every `threshold` after.
///
/// `label` should identify the op and its key params (thread id, stream ptr, gate state).
pub fn watch(label: impl Into<String>, threshold: Duration) -> WatchGuard {
    ensure_started();
    let reg = registry();
    let id = reg.next_id.fetch_add(1, Ordering::Relaxed);
    let inflight = InFlight {
        label: label.into(),
        start: Instant::now(),
        threshold,
        last_reported_secs: 0,
    };
    if let Ok(mut ops) = reg.ops.lock() {
        ops.insert(id, inflight);
    }
    WatchGuard { id }
}
