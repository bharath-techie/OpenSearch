#!/usr/bin/env bash
#
# Work-stealing OFF vs ON A/B over the 86 match()-augmented ClickBench queries
# (43 × {nonsel, sel}). match() forces the indexed/delegation path (Lucene
# round-trip per chunk), which is where work-stealing's collectors live — so this
# stresses the feature far more than pure-parquet ClickBench.
#
#   nonsel: AND match(URL,'http')   ~98.8% match → maximal delegation load
#   sel:    AND match(URL,'yandex') ~0.01% match → heavy row-group pruning (imbalance)
#
# Same build, toggle the dynamic `datafusion.indexed.work_stealing` between arms.
# Multi-partition forced (mode=auto, max_slice_count=N → target_partitions=min(N,cores)).
#
# Usage:  bash run_work_stealing_match_ab.sh [TARGET_PARTITIONS]   (default 8)
#   Results: work-stealing-match-ab-results/p<N>/{sel,nonsel}/{bench_off,bench_on}.json
#            + COMPARISON.md per variant.
set -uo pipefail

REPO=/Users/gbh/Documents/dev/OpenSearch
MATCH_DIR=$REPO/sandbox/plugins/analytics-backend-datafusion/rust/docs/ppl_match
TARGET_PARTITIONS=${1:-8}
OUTROOT=$REPO/sandbox/plugins/analytics-backend-datafusion/rust/docs/work-stealing-match-ab-results/p$TARGET_PARTITIONS
NODE=http://localhost:9200
mkdir -p "$OUTROOT"
RUNNER=/tmp/bench_ws_match.py
log() { echo "[$(date '+%H:%M:%S')] $*"; }
set_cluster() { curl -s -X PUT "$NODE/_cluster/settings" -H 'Content-Type: application/json' -d "$1"; }

# ---- preflight ----
status=$(curl -s "$NODE/_cat/indices/clickbench?h=health,docs.count" 2>/dev/null)
echo "$status" | grep -qE '^(yellow|green)' || { log "ABORT: clickbench not ready ($status)"; exit 1; }
probe=$(set_cluster '{"persistent":{"datafusion.indexed.work_stealing":false}}')
echo "$probe" | grep -q "not recognized" && { log "ABORT: node doesn't recognize work_stealing setting (stale jar)"; echo "$probe"; exit 1; }
# clear any stale archived.* settings that block updates, then force multi-partition
set_cluster '{"persistent":{"archived.*":null}}' >/dev/null
log "multi-partition: mode=auto, max_slice_count=$TARGET_PARTITIONS"
set_cluster "{\"persistent\":{\"search.concurrent_segment_search.mode\":\"auto\",\"search.concurrent.max_slice_count\":$TARGET_PARTITIONS}}" >/dev/null

# ---- per-arm runner (reads a query dir, runs all 43 q*.ppl) ----
cat > "$RUNNER" <<'PYEOF'
import json, time, sys, urllib.request, urllib.error, os
QDIR, LABEL, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
NODE = "http://localhost:9200"
def post(path, body=None):
    data = body.encode() if body else None
    req = urllib.request.Request(NODE+path, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=90) as r: return r.read().decode()
def run_query(ppl):
    t0=time.time()
    try:
        j=json.loads(post("/_plugins/_ppl", json.dumps({"query":ppl})))
        dt=time.time()-t0
        if "error" in j: return dt,None,json.dumps(j["error"])[:300]
        return dt,j.get("datarows"),None
    except urllib.error.HTTPError as e: return time.time()-t0,None,f"HTTP {e.code}: {e.read().decode()[:200]}"
    except Exception as e: return time.time()-t0,None,f"{type(e).__name__}: {str(e)[:200]}"
def clear_cache():
    try: post("/clickbench/_cache/clear")
    except Exception as e: print("cache clear failed:",e)
results={}
for i in range(1,44):
    name=f"q{i}"; ppl=open(os.path.join(QDIR,f"{name}.ppl")).read().strip()
    # Warm-only: 1 discarded warmup, then 1 measured warm (no cold cache-clear — the
    # nonsel match touches ~99% of docs and cold runs are punishingly slow).
    w0_t,w0_rows,w0_err=run_query(ppl)
    w_t,w_rows,w_err=run_query(ppl)
    rows=w_rows if w_rows is not None else w0_rows
    err=w_err or w0_err
    best=w_t if w_err is None else None
    results[name]={"ppl":ppl,"warmup_s":round(w0_t,4),"best_warm_s":round(best,4) if best else None,
        "row_count":len(rows) if rows is not None else None,"rows":rows,"error":err}
    bw=f"{best:.3f}s" if best else "n/a"
    print(f"{name:>4}: warmup={w0_t:6.3f}s warm={bw:>8} rows={results[name]['row_count']} {'OK' if not err else 'ERR'}")
json.dump({"label":LABEL,"results":results}, open(OUT,"w"), indent=2)
print(f"wrote {OUT}")
PYEOF

# ---- compare report per variant ----
cat > /tmp/ws_match_compare.py <<'PYEOF'
import json, os, sys
OUT=os.environ["WS_OUTDIR"]; VAR=sys.argv[1]
A=json.load(open(f"{OUT}/bench_off.json"))["results"]
B=json.load(open(f"{OUT}/bench_on.json"))["results"]
def norm(r): return None if r is None else json.dumps(r, sort_keys=True)
L=[f"# Work-stealing OFF vs ON — match() variant `{VAR}` (43 queries)\n",
   "match() forces the indexed/delegation (Lucene) path. Multi-partition; same build, only `datafusion.indexed.work_stealing` differs.\n",
   "Correctness = byte-equal rows OFF vs ON (mismatches on `head`-without-total-sort are benign ties — verify the sort-key prefix).\n",
   "\n| Query | rows(off) | rows(on) | match | off warm | on warm | on/off |","|---|---|---|---|---|---|---|"]
mism=0; toff=ton=0.0
for q in [f"q{i}" for i in range(1,44)]:
    a,b=A[q],B[q]
    ae,be=a.get("error"),b.get("error")
    aw,bw=a.get("best_warm_s"),b.get("best_warm_s")
    if ae or be:
        L.append(f"| {q} | {'ERR' if ae else a['row_count']} | {'ERR' if be else b['row_count']} | ⚠️ | {aw} | {bw} | - |"); mism+=1; continue
    same=norm(a.get("rows"))==norm(b.get("rows"))
    if aw and bw: toff+=aw; ton+=bw; ratio=f"{bw/aw:.2f}x"
    else: ratio="-"
    L.append(f"| {q} | {a['row_count']} | {b['row_count']} | {'✅' if same else '❌'} | {aw} | {bw} | {ratio} |")
    if not same: mism+=1
ratio = ton/toff if toff else 0
L.append(f"\n**Flagged (mismatch or error): {mism}/43**  (inspect ❌ for benign LIMIT-ties vs real)")
L.append(f"**Total warm: off={toff:.2f}s on={ton:.2f}s on/off={ratio:.3f}x ({'ON faster' if ratio<1 else 'ON slower'} {abs(1-ratio)*100:.1f}%)**\n")
open(f"{OUT}/COMPARISON.md","w").write("\n".join(L)); print("\n".join(L))
PYEOF

# ---- run both variants × both arms ----
for VAR in nonsel sel; do
  OUTDIR=$OUTROOT/$VAR; mkdir -p "$OUTDIR"; export WS_OUTDIR="$OUTDIR"
  log "============ VARIANT: $VAR ============"
  log "ARM OFF (static)"; set_cluster '{"persistent":{"datafusion.indexed.work_stealing":false}}' >/dev/null; sleep 2
  python3 -u "$RUNNER" "$MATCH_DIR/$VAR" off "$OUTDIR/bench_off.json" | tee "$OUTDIR/console_off.txt"
  log "ARM ON (stealing)"; set_cluster '{"persistent":{"datafusion.indexed.work_stealing":true}}' >/dev/null; sleep 2
  python3 -u "$RUNNER" "$MATCH_DIR/$VAR" on "$OUTDIR/bench_on.json" | tee "$OUTDIR/console_on.txt"
  python3 /tmp/ws_match_compare.py "$VAR" | tee "$OUTDIR/COMPARISON.txt"
done

# ---- restore ----
log "restoring concurrency settings to defaults"
set_cluster '{"persistent":{"search.concurrent_segment_search.mode":null,"search.concurrent.max_slice_count":null}}' >/dev/null
log "DONE. Results in $OUTROOT/{nonsel,sel}/"
