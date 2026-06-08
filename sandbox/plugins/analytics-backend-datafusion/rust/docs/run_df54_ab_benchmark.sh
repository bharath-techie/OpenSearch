#!/usr/bin/env bash
#
# DF53 (main) vs DF54 (df-54-upgrade) A/B benchmark for the 43 clickbench PPL queries.
#
# What it does, fully unattended:
#   For each branch in [main, df-54-upgrade]:
#     1. git checkout <branch>
#     2. cargo build --release       (only the Rust dylib differs between branches)
#     3. restart the archive node     (jvm.options already -> target/release)
#     4. wait until clickbench index is ready (yellow/green, ~100M docs)
#     5. run all 43 PPL queries: 1 cold (cache cleared) + 2 warm; record wall-clock + result rows
#   Then compare results (correctness) + best-of-2-warm wall-clock (perf), write a report.
#
# NOTE: main and df-54-upgrade differ ONLY in Rust (.rs + Cargo.toml/lock); the Java
#       core/plugins/modules are identical, so only the dylib is rebuilt per branch.
#       The archive's lib/ modules/ plugins/ stay as-is (current synced revision).
#
# Usage:  bash run_df54_ab_benchmark.sh
# Re-run-safe. All output lands in $OUTDIR.
set -uo pipefail

# ---- paths -------------------------------------------------------------------
REPO=/Users/gbh/Documents/dev/OpenSearch
RUST=$REPO/sandbox/libs/dataformat-native/rust
ARCHIVE=/Users/gbh/Documents/data-fol/3.7.0-ARCHIVE
JVMOPTS=$ARCHIVE/config/jvm.options
PPL_DIR=$REPO/sandbox/qa/analytics-engine-rest/src/test/resources/datasets/clickbench/ppl
OUTDIR=$REPO/sandbox/plugins/analytics-backend-datafusion/rust/docs/df54-benchmark-results
NODE=http://localhost:9200
RELEASE_PATH=$RUST/target/release

mkdir -p "$OUTDIR"
RUNNER=/tmp/bench_clickbench.py   # written by this script below

# ---- helpers -----------------------------------------------------------------
log() { echo "[$(date '+%H:%M:%S')] $*"; }

stop_node() {
  if pgrep -f Dopensearch >/dev/null; then
    log "stopping node..."; kill "$(pgrep -f Dopensearch)" 2>/dev/null; sleep 5
    pgrep -f Dopensearch >/dev/null && { kill -9 "$(pgrep -f Dopensearch)" 2>/dev/null; sleep 3; }
  fi
}

start_node() {
  local logf=$1
  log "starting node (log: $logf)"
  ( cd "$ARCHIVE" && ./bin/opensearch > "$logf" 2>&1 & )
}

wait_ready() {
  local logf=$1 i status
  for i in $(seq 1 30); do            # up to ~300s
    # bail early on a fatal startup crash
    if grep -qE 'ExceptionInInitializerError|UnsatisfiedLinkError|NoSuchMethodError|NoClassDefFoundError|fatal error in thread' "$logf" 2>/dev/null; then
      log "FATAL startup error detected in $logf:"; grep -nE 'Error|Exception|fatal' "$logf" | head -8; return 1
    fi
    status=$(curl -s "$NODE/_cat/indices/clickbench?h=health,docs.count" 2>/dev/null)
    if echo "$status" | grep -qE '^(yellow|green)'; then
      log "node ready: $status"; return 0
    fi
    sleep 10
  done
  log "TIMEOUT waiting for node ready"; return 1
}

build_release() {
  log "cargo build --release (this can take a few min)..."
  ( cd "$RUST" && cargo build --release ) > "$OUTDIR/cargo_build_$1.log" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then log "BUILD FAILED (rc=$rc), see $OUTDIR/cargo_build_$1.log"; tail -20 "$OUTDIR/cargo_build_$1.log"; fi
  return $rc
}

run_branch() {            # $1 = branch, $2 = label
  local branch=$1 label=$2
  log "==================== BRANCH: $branch (label=$label) ===================="
  # cargo build regenerates Cargo.lock; discard that so branch switch is clean.
  ( cd "$REPO" && git checkout -- sandbox/libs/dataformat-native/rust/Cargo.lock 2>/dev/null )
  ( cd "$REPO" && git checkout "$branch" ) || { log "git checkout $branch FAILED"; return 1; }
  log "HEAD now: $(cd "$REPO" && git rev-parse --short HEAD) ($(cd "$REPO" && git rev-parse --abbrev-ref HEAD))"
  build_release "$label" || return 1
  ls -la "$RELEASE_PATH/libopensearch_native.dylib" | tee -a "$OUTDIR/run.log"
  stop_node
  start_node "/tmp/os-$label.log"
  wait_ready "/tmp/os-$label.log" || return 1
  log "running 43 queries for $label ..."
  python3 "$RUNNER" "$label" "$OUTDIR/bench_$label.json" | tee "$OUTDIR/console_$label.txt"
}

# ---- write the python query runner ------------------------------------------
cat > "$RUNNER" <<'PYEOF'
import json, time, sys, urllib.request, urllib.error, os
PPL_DIR = "/Users/gbh/Documents/dev/OpenSearch/sandbox/qa/analytics-engine-rest/src/test/resources/datasets/clickbench/ppl"
NODE = "http://localhost:9200"
LABEL = sys.argv[1] if len(sys.argv) > 1 else "run"
OUT = sys.argv[2] if len(sys.argv) > 2 else f"/tmp/bench_{LABEL}.json"
def post(path, body=None):
    data = body.encode() if body else None
    req = urllib.request.Request(NODE+path, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=300) as r: return r.read().decode()
def run_query(ppl):
    t0=time.time()
    try:
        j=json.loads(post("/_plugins/_ppl", json.dumps({"query":ppl})))
        dt=time.time()-t0
        if "error" in j: return dt,None,json.dumps(j["error"])[:300]
        return dt,j.get("datarows"),None
    except urllib.error.HTTPError as e:
        return time.time()-t0,None,f"HTTP {e.code}: {e.read().decode()[:300]}"
    except Exception as e:
        return time.time()-t0,None,f"{type(e).__name__}: {str(e)[:300]}"
def clear_cache():
    try: post("/clickbench/_cache/clear")
    except Exception as e: print("cache clear failed:",e)
results={}
queries=[(f"q{i}",open(os.path.join(PPL_DIR,f"q{i}.ppl")).read().strip()) for i in range(1,44)]
for name,ppl in queries:
    clear_cache()
    c_t,c_rows,c_err=run_query(ppl)
    w1_t,w1_rows,w1_err=run_query(ppl)
    w2_t,w2_rows,w2_err=run_query(ppl)
    warm=[t for t,e in [(w1_t,w1_err),(w2_t,w2_err)] if e is None]
    best_warm=min(warm) if warm else None
    rows=next((r for r in [w2_rows,w1_rows,c_rows] if r is not None),None)
    err=c_err or w1_err or w2_err
    results[name]={"ppl":ppl,"cold_s":round(c_t,4),"warm1_s":round(w1_t,4),"warm2_s":round(w2_t,4),
        "best_warm_s":round(best_warm,4) if best_warm else None,
        "row_count":len(rows) if rows is not None else None,"rows":rows,"error":err}
    bw=f"{best_warm:.3f}s" if best_warm else "n/a"
    print(f"{name:>4}: cold={c_t:6.3f}s warm_best={bw:>8}  rows={results[name]['row_count']}  {'OK' if err is None else 'ERR:'+err[:60]}")
json.dump({"label":LABEL,"results":results}, open(OUT,"w"), indent=2)
print(f"\nWrote {OUT}")
PYEOF

# ---- comparison report ------------------------------------------------------
write_report() {
cat > /tmp/bench_compare.py <<'PYEOF'
import json
A=json.load(open("/Users/gbh/Documents/dev/OpenSearch/sandbox/plugins/analytics-backend-datafusion/rust/docs/df54-benchmark-results/bench_main.json"))["results"]
B=json.load(open("/Users/gbh/Documents/dev/OpenSearch/sandbox/plugins/analytics-backend-datafusion/rust/docs/df54-benchmark-results/bench_df54.json"))["results"]
APPROX={"q5","q6","q9","q10","q11","q12","q14","q23"}  # use dc() approx-distinct
def norm(rows):
    if rows is None: return None
    try: return json.dumps(rows, sort_keys=True)
    except Exception: return str(rows)
lines=[]
lines.append("# DF53 (main) vs DF54 (df-54-upgrade) — ClickBench 43-query A/B\n")
lines.append("Node: archive, ~99,997,497 docs. Per query: 1 cold (cache cleared) + 2 warm; perf = best of 2 warm.\n")
lines.append("Correctness = result-row equality between branches. `dc()` queries use approximate distinct-count (may differ slightly) and are flagged ~APPROX.\n")
# correctness
lines.append("\n## Correctness\n")
lines.append("| Query | rows(main) | rows(df54) | match | note |")
lines.append("|---|---|---|---|---|")
mism=0
for q in [f"q{i}" for i in range(1,44)]:
    a,b=A.get(q,{}),B.get(q,{})
    ae,be=a.get("error"),b.get("error")
    if ae or be:
        lines.append(f"| {q} | {'ERR' if ae else a.get('row_count')} | {'ERR' if be else b.get('row_count')} | ⚠️ | {'main:'+ae[:40] if ae else ''} {'df54:'+be[:40] if be else ''} |")
        mism+=1; continue
    same = norm(a.get("rows"))==norm(b.get("rows"))
    note="~APPROX" if q in APPROX and not same else ""
    mark="✅" if same else ("≈" if q in APPROX else "❌")
    if not same and q not in APPROX: mism+=1
    lines.append(f"| {q} | {a.get('row_count')} | {b.get('row_count')} | {mark} | {note} |")
lines.append(f"\n**Non-approx mismatches/errors: {mism}**\n")
# perf
lines.append("\n## Performance (best-of-2-warm, seconds)\n")
lines.append("| Query | main warm | df54 warm | Δ (df54-main) | df54/main | main cold | df54 cold |")
lines.append("|---|---|---|---|---|---|---|")
tm=td=0.0
for q in [f"q{i}" for i in range(1,44)]:
    a,b=A.get(q,{}),B.get(q,{})
    aw,bw=a.get("best_warm_s"),b.get("best_warm_s")
    ac,bc=a.get("cold_s"),b.get("cold_s")
    if aw and bw:
        tm+=aw; td+=bw
        d=bw-aw; r=bw/aw if aw else 0
        lines.append(f"| {q} | {aw:.3f} | {bw:.3f} | {d:+.3f} | {r:.2f}x | {ac:.3f} | {bc:.3f} |")
    else:
        lines.append(f"| {q} | {aw} | {bw} | - | - | {ac} | {bc} |")
lines.append(f"\n**Total warm (sum best-of-2): main={tm:.2f}s  df54={td:.2f}s  ratio={td/tm:.3f}x**\n")
open("/Users/gbh/Documents/dev/OpenSearch/sandbox/plugins/analytics-backend-datafusion/rust/docs/df54-benchmark-results/COMPARISON.md","w").write("\n".join(lines))
print("\n".join(lines))
PYEOF
python3 /tmp/bench_compare.py | tee "$OUTDIR/COMPARISON.txt"
}

# ============================== MAIN ==========================================
log "OUTDIR=$OUTDIR"
# Phase 0: jvm.options -> target/release
if grep -q 'target/debug' "$JVMOPTS"; then
  sed -i '' 's#target/debug#target/release#' "$JVMOPTS"
  log "jvm.options switched to target/release"
fi
grep 'java.library.path' "$JVMOPTS" | tee -a "$OUTDIR/run.log"

# guard: tree must be clean to switch branches
if [ -n "$(cd "$REPO" && git status --porcelain | grep '^ M')" ]; then
  log "ERROR: working tree has modified tracked files; commit/stash before running."; exit 1
fi
START_BRANCH=$(cd "$REPO" && git rev-parse --abbrev-ref HEAD)
log "starting branch was: $START_BRANCH"

run_branch main main           || { log "main run failed"; }
run_branch df-54-upgrade df54  || { log "df54 run failed"; }

# restore starting branch
( cd "$REPO" && git checkout "$START_BRANCH" )

log "writing comparison report..."
write_report
log "DONE. Results in $OUTDIR (COMPARISON.md, bench_main.json, bench_df54.json)"
