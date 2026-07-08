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

/// How often the consolidated [`WATCHDOG-SNAPSHOT`] dump re-fires while a wedge
/// persists, in seconds. We rate-limit to this cadence (keyed off the oldest
/// stuck op's age) so a permanent wedge re-dumps the full holder/waiter picture
/// periodically instead of every tick (flood) or only once (stale).
const SNAPSHOT_INTERVAL_SECS: u64 = 15;

/// Age (seconds) at which the watchdog auto-fires the tokio TASK DUMP. Chosen ABOVE
/// the observed healthy-slow-query ceiling (~85s) so it never fires on a query that
/// is merely slow: anything blocked this long is a confirmed permanent wedge. Because
/// the wedge is permanent, the parked `.await` is stable, so dumping at 120s captures
/// the same frame as dumping at 1600s — but earlier is better (fewer piled-up waiters
/// muddying the runtime). Taken ONCE per wedge episode (see the `dump_taken` latch).
const TASKDUMP_THRESHOLD_SECS: u64 = 120;

/// Hook the plugin installs at init so the watchdog (which lives in this dependency-free
/// common crate) can trigger a tokio task dump without a circular dep on the plugin crate.
/// `fn(reason)` — the plugin-side fn owns choosing the output path + driving `Handle::dump()`.
static DUMP_HOOK: OnceLock<fn(&str)> = OnceLock::new();

/// Install the task-dump hook. Called once from the plugin at runtime-manager init.
/// The hook must be a plain `fn` (no captured state) — it reaches the runtimes via the
/// plugin's own globals. Idempotent; a second set is ignored.
pub fn set_dump_hook(hook: fn(&str)) {
    let _ = DUMP_HOOK.set(hook);
}

/// Build a snapshot of every in-flight op — stuck or not — as `(elapsed, label)`
/// pairs sorted oldest-first. The oldest ops (top of the list) are the likely
/// *holders* (a `stream_next` / `execute-with-context` that has been blocked for
/// minutes, pinning a gate permit); the youngest (bottom) are the *waiters* (a
/// fresh `gate-acquire` that just started blocking because the holders never
/// released). Reading top-to-bottom answers "who is waiting on what".
fn build_snapshot(ops: &HashMap<u64, InFlight>) -> Vec<(Duration, String)> {
    let mut entries: Vec<(Duration, String)> = ops
        .values()
        .map(|f| (f.start.elapsed(), f.label.clone()))
        .collect();
    // Oldest first.
    entries.sort_by(|a, b| b.0.cmp(&a.0));
    entries
}

/// Render a snapshot into a single multi-line `[WATCHDOG-SNAPSHOT]` message.
fn render_snapshot(entries: &[(Duration, String)]) -> String {
    let mut msg = format!(
        "[WATCHDOG-SNAPSHOT] {} native op(s) in flight (oldest first; likely permit-HOLDERS at top, \
         WAITERS at bottom):",
        entries.len()
    );
    for (elapsed, label) in entries {
        msg.push_str(&format!("\n  age={:>8.1}s  {}", elapsed.as_secs_f64(), label));
    }
    msg
}

fn watchdog_loop() {
    let reg = registry();
    // Bucket (in units of SNAPSHOT_INTERVAL_SECS) of the oldest stuck op at the
    // last consolidated snapshot. Resets to 0 whenever nothing is stuck, so the
    // next wedge dumps promptly.
    let mut last_snapshot_bucket: u64 = 0;
    // Latch so the auto task-dump fires at most once per wedge episode; re-armed when
    // nothing is stuck (see the `None` arm below).
    let mut dump_taken = false;
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
        // Pass 1: per-op re-warn, and track the oldest stuck op this tick.
        let mut oldest_stuck: Option<Duration> = None;
        for inflight in guard.values_mut() {
            let elapsed = inflight.start.elapsed();
            if elapsed < inflight.threshold {
                continue;
            }
            oldest_stuck = Some(oldest_stuck.map_or(elapsed, |o: Duration| o.max(elapsed)));
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
        // Pass 2: while anything is stuck, emit ONE consolidated snapshot of all
        // in-flight ops (holders + waiters) so the correlation is legible in a
        // single log block. Rate-limited to SNAPSHOT_INTERVAL_SECS via the oldest
        // stuck op's age, and re-fires as the wedge ages.
        match oldest_stuck {
            Some(oldest) => {
                let bucket = oldest.as_secs() / SNAPSHOT_INTERVAL_SECS.max(1);
                if bucket > last_snapshot_bucket {
                    last_snapshot_bucket = bucket;
                    let snapshot = build_snapshot(&guard);
                    logger::log(LogLevel::Error, &render_snapshot(&snapshot));
                }
                // Auto task-dump: once the oldest stuck op crosses the wedge threshold,
                // fire the plugin's dump hook a single time this episode. This captures the
                // parked `.await` of the wedged producer — the definitive lost-wakeup locus.
                // Drop the ops lock first: the hook drives block_on/Handle::dump which can take
                // seconds, and we must not hold the registry mutex across it (would stall
                // register/deregister of every other FFM op).
                if !dump_taken && oldest.as_secs() >= TASKDUMP_THRESHOLD_SECS {
                    dump_taken = true;
                    if let Some(hook) = DUMP_HOOK.get().copied() {
                        let reason = format!(
                            "watchdog auto-dump: op stuck {:.0}s (>= {}s wedge threshold)",
                            oldest.as_secs_f64(),
                            TASKDUMP_THRESHOLD_SECS
                        );
                        drop(guard);
                        logger::log(
                            LogLevel::Error,
                            &format!("[WATCHDOG-TASKDUMP] firing tokio task dump — {reason}"),
                        );
                        hook(&reason);
                        continue; // guard already dropped; restart the loop cleanly
                    } else {
                        logger::log(
                            LogLevel::Error,
                            "[WATCHDOG-TASKDUMP] wedge threshold hit but no dump hook installed",
                        );
                    }
                }
            }
            None => {
                last_snapshot_bucket = 0;
                dump_taken = false; // episode over — arm the dump for the next wedge
            }
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

/// Snapshot of every currently-registered in-flight op as `(elapsed, label)`
/// pairs, oldest-first — the same ordering the consolidated `[WATCHDOG-SNAPSHOT]`
/// log uses (likely permit-holders at the top, waiters at the bottom).
///
/// Exposed so callers (and tests) can read the live "who is waiting on what"
/// picture on demand without waiting for the ticker thread to log it.
pub fn snapshot() -> Vec<(Duration, String)> {
    let reg = registry();
    let guard = match reg.ops.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    build_snapshot(&guard)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A `HashMap<u64, InFlight>` with entries whose `start` is `age_secs` in the
    /// past, so `build_snapshot` sees them as long-running.
    fn inflight_map(entries: &[(&str, u64)]) -> HashMap<u64, InFlight> {
        let mut map = HashMap::new();
        for (i, (label, age_secs)) in entries.iter().enumerate() {
            map.insert(
                i as u64,
                InFlight {
                    label: label.to_string(),
                    start: Instant::now() - Duration::from_secs(*age_secs),
                    threshold: Duration::from_secs(5),
                    last_reported_secs: 0,
                },
            );
        }
        map
    }

    // build_snapshot must order oldest-first: the permit HOLDER (oldest stuck op)
    // sorts above the WAITERS that started blocking later because it never released.
    #[test]
    fn snapshot_orders_oldest_first() {
        let map = inflight_map(&[
            ("[gate-acquire] ctx=200 1 permits", 3),   // youngest — a waiter
            ("[stream-next] ctx=100 — CPU not producing", 300), // oldest — the holder
            ("[gate-acquire] ctx=300 1 permits", 10),
        ]);
        let snap = build_snapshot(&map);
        assert_eq!(snap.len(), 3);
        // Oldest (the holder) is first.
        assert!(snap[0].1.contains("ctx=100"), "holder should sort first: {:?}", snap);
        // Youngest waiter is last.
        assert!(snap[2].1.contains("ctx=200"), "youngest waiter should sort last: {:?}", snap);
        // Monotonically non-increasing age.
        assert!(snap[0].0 >= snap[1].0 && snap[1].0 >= snap[2].0);
    }

    // The rendered snapshot must be a single message that names every in-flight op
    // and their query ids, so the whole holder/waiter picture is legible in one block.
    #[test]
    fn render_snapshot_lists_all_ops_and_ctx_ids() {
        let map = inflight_map(&[
            ("[stream-next] ctx=100 — CPU not producing", 300),
            ("[gate-acquire] ctx=200 1 permits", 3),
        ]);
        let out = render_snapshot(&build_snapshot(&map));
        assert!(out.starts_with("[WATCHDOG-SNAPSHOT] 2 native op(s) in flight"), "{out}");
        assert!(out.contains("ctx=100"), "{out}");
        assert!(out.contains("ctx=200"), "{out}");
        // Holder (ctx=100) appears before the waiter (ctx=200) in the text.
        assert!(out.find("ctx=100").unwrap() < out.find("ctx=200").unwrap());
    }

    // The live registry snapshot reflects registered guards and drops them on scope exit.
    #[test]
    fn live_snapshot_tracks_guards() {
        let a = watch("[test-op-A] ctx=7", Duration::from_secs(5));
        let b = watch("[test-op-B] ctx=8", Duration::from_secs(5));
        let labels: Vec<String> = snapshot().into_iter().map(|(_, l)| l).collect();
        assert!(labels.iter().any(|l| l.contains("ctx=7")));
        assert!(labels.iter().any(|l| l.contains("ctx=8")));
        drop(a);
        drop(b);
        // After both guards drop, neither op is registered any longer.
        let labels: Vec<String> = snapshot().into_iter().map(|(_, l)| l).collect();
        assert!(!labels.iter().any(|l| l.contains("ctx=7") || l.contains("ctx=8")));
    }
}
