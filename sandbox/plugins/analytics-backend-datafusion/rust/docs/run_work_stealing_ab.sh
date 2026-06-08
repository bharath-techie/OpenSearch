#!/usr/bin/env bash
#
# Work-stealing OFF vs ON A/B for the 43 clickbench PPL queries.
#
# Unlike the DF53/DF54 branch A/B, both arms use the SAME deployed build (dylib +
# jar). Work-stealing is a *dynamic cluster setting*, so we just toggle it between
# arms — no rebuild, no node restart, no branch switch.
#
#   Arm A: datafusion.indexed.work_stealing = false  (static per-partition)
#   Arm B: datafusion.indexed.work_stealing = true   (shared queue / stealing)
#
# CRITICAL: work-stealing only does anything with MULTIPLE partitions. With one
# partition the shared queue has one consumer and OFF==ON trivially. So we force
# multi-partition concurrency before running. target_partitions is derived by
# DatafusionSettings.deriveTargetPartitions(mode, max_slice_count):
#   mode=none            -> 1
#   max_slice_count=0    -> cores/2
#   else                 -> min(max_slice_count, cores)
# So for exactly 4 partitions on a >=4-core box:
#   - search.concurrent_segment_search.mode      = auto   (NOT none)
#   - search.concurrent.max_slice_count          = 4      -> min(4, cores) = 4
# Confirm in an EXPLAIN that QueryShardExec shows partitions=4.
#
# Correctness: results must be BYTE-IDENTICAL between OFF and ON (work-stealing
# only changes which thread does which chunk, never the rows). dc() approx-distinct
# queries can vary by a hair across runs and are flagged ~APPROX, but should still
# match since the build is identical.
#
# Prereq (USER HANDLES DEPLOY): the archive node must already be running a build
# whose jar recognizes `datafusion.indexed.work_stealing` (probe below verifies it,
# and aborts with guidance if not). The dylib must be the release build with the
# work-stealing changes.
#
# Usage:  bash run_work_stealing_ab.sh [TARGET_PARTITIONS]
#   TARGET_PARTITIONS defaults to 4. Results land in a per-partition-count subdir
#   (work-stealing-ab-results/p<N>) so multiple runs don't clobber each other.
set -uo pipefail

REPO=/Users/gbh/Documents/dev/OpenSearch
PPL_DIR=$REPO/sandbox/qa/analytics-engine-rest/src/test/resources/datasets/clickbench/ppl
TARGET_PARTITIONS=${1:-4}   # via search.concurrent.max_slice_count (min(N, cores))
OUTDIR=$REPO/sandbox/plugins/analytics-backend-datafusion/rust/docs/work-stealing-ab-results/p$TARGET_PARTITIONS
NODE=http://localhost:9200

mkdir -p "$OUTDIR"
RUNNER=/tmp/bench_ws_clickbench.py
log() { echo "[$(date '+%H:%M:%S')] $*"; }

set_cluster() { # $1 json body
  curl -s -X PUT "$NODE/_cluster/settings" -H 'Content-Type: application/json' -d "$1"
}

# ---- 0. preflight: node up + setting recognized + multi-partition --------------
status=$(curl -s "$NODE/_cat/indices/clickbench?h=health,docs.count" 2>/dev/null)
if ! echo "$status" | grep -qE '^(yellow|green)'; then
  log "ABORT: clickbench index not ready (got: '$status'). Is the archive node up?"; exit 1
fi
log "clickbench ready: $status"

probe=$(set_cluster '{"persistent":{"datafusion.indexed.work_stealing":false}}')
if echo "$probe" | grep -q "not recognized"; then
  log "ABORT: node does not recognize datafusion.indexed.work_stealing."
  log "       The deployed plugin jar is stale. Deploy a build whose jar wires the"
  log "       setting, restart the node, and re-run. (USER: you said you'd handle deploy.)"
  echo "$probe"; exit 1
fi
log "setting recognized ✓"

# Force multi-partition concurrency (else work-stealing is a no-op).
log "enabling multi-partition concurrency (mode=auto, max_slice_count=$TARGET_PARTITIONS)"
set_cluster "{\"persistent\":{
  \"search.concurrent_segment_search.mode\":\"auto\",
  \"search.concurrent.max_slice_count\":$TARGET_PARTITIONS
}}" >/dev/null

# Sanity: dump an EXPLAIN so we can eyeball partitions=N in the report.
curl -s -X POST "$NODE/_plugins/_ppl/_explain" -H 'Content-Type: application/json' \
  -d '{"query":"source = clickbench | where AdvEngineID!=0 | stats count() by AdvEngineID"}' \
  > "$OUTDIR/explain_multipartition.json" 2>/dev/null
log "wrote EXPLAIN to $OUTDIR/explain_multipartition.json (verify partitions>1)"

# ---- 1. the per-arm query runner --------------------------------------------
cat > "$RUNNER" <<'PYEOF'
import json, time, sys, urllib.request, urllib.error, os
PPL_DIR = "/Users/gbh/Documents/dev/OpenSearch/sandbox/qa/analytics-engine-rest/src/test/resources/datasets/clickbench/ppl"
NODE = "http://localhost:9200"
LABEL = sys.argv[1]
OUT   = sys.argv[2]
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

# ---- 2. run both arms (toggle the dynamic setting between them) -------------
log "==== ARM A: work_stealing = false (static) ===="
set_cluster '{"persistent":{"datafusion.indexed.work_stealing":false}}' >/dev/null
sleep 2
python3 "$RUNNER" off "$OUTDIR/bench_off.json" | tee "$OUTDIR/console_off.txt"

log "==== ARM B: work_stealing = true (stealing) ===="
set_cluster '{"persistent":{"datafusion.indexed.work_stealing":true}}' >/dev/null
sleep 2
python3 "$RUNNER" on "$OUTDIR/bench_on.json" | tee "$OUTDIR/console_on.txt"

# ---- 3. comparison report ---------------------------------------------------
export WS_OUTDIR="$OUTDIR"
cat > /tmp/ws_compare.py <<'PYEOF'
import json, os
OUT=os.environ["WS_OUTDIR"]
A=json.load(open(f"{OUT}/bench_off.json"))["results"]   # OFF / static
B=json.load(open(f"{OUT}/bench_on.json"))["results"]    # ON  / stealing
APPROX={"q5","q6","q9","q10","q11","q12","q14","q23"}
def norm(rows):
    if rows is None: return None
    try: return json.dumps(rows, sort_keys=True)
    except Exception: return str(rows)
L=[]
L.append("# Work-stealing OFF vs ON — ClickBench 43-query A/B\n")
L.append("Node: archive, ~99,997,497 docs. Multi-partition (mode=auto, min_target_partitions>=4). Same build; only the dynamic `datafusion.indexed.work_stealing` setting differs between arms.\n")
L.append("Per query: 1 cold (cache cleared) + 2 warm; perf = best of 2 warm. Correctness = byte-equal result rows OFF vs ON.\n")
L.append("\n## Correctness (OFF vs ON must match)\n")
L.append("| Query | rows(off) | rows(on) | match | note |")
L.append("|---|---|---|---|---|")
mism=0
for q in [f"q{i}" for i in range(1,44)]:
    a,b=A.get(q,{}),B.get(q,{})
    ae,be=a.get("error"),b.get("error")
    if ae or be:
        L.append(f"| {q} | {'ERR' if ae else a.get('row_count')} | {'ERR' if be else b.get('row_count')} | ⚠️ | {('off:'+ae[:40]) if ae else ''} {('on:'+be[:40]) if be else ''} |")
        mism+=1; continue
    same = norm(a.get("rows"))==norm(b.get("rows"))
    mark="✅" if same else ("≈" if q in APPROX else "❌")
    note="~APPROX" if (q in APPROX and not same) else ""
    if not same and q not in APPROX: mism+=1
    L.append(f"| {q} | {a.get('row_count')} | {b.get('row_count')} | {mark} | {note} |")
L.append(f"\n**Non-approx mismatches/errors: {mism}**  (must be 0 — work-stealing must never change results)\n")
L.append("\n## Performance (best-of-2-warm, seconds)\n")
L.append("| Query | off warm | on warm | Δ (on-off) | on/off | off cold | on cold |")
L.append("|---|---|---|---|---|---|---|")
toff=ton=0.0
for q in [f"q{i}" for i in range(1,44)]:
    a,b=A.get(q,{}),B.get(q,{})
    aw,bw=a.get("best_warm_s"),b.get("best_warm_s")
    ac,bc=a.get("cold_s"),b.get("cold_s")
    if aw and bw:
        toff+=aw; ton+=bw
        L.append(f"| {q} | {aw:.3f} | {bw:.3f} | {bw-aw:+.3f} | {bw/aw:.2f}x | {ac:.3f} | {bc:.3f} |")
    else:
        L.append(f"| {q} | {aw} | {bw} | - | - | {ac} | {bc} |")
ratio = ton/toff if toff else 0
L.append(f"\n**Total warm (sum best-of-2): off={toff:.2f}s  on={ton:.2f}s  on/off={ratio:.3f}x**")
L.append(f"({'ON faster' if ratio<1 else 'ON slower'} overall by {abs(1-ratio)*100:.1f}%)\n")
open(f"{OUT}/COMPARISON.md","w").write("\n".join(L))
print("\n".join(L))
PYEOF
python3 /tmp/ws_compare.py | tee "$OUTDIR/COMPARISON.txt"

# ---- 4. restore concurrency settings to defaults (leave work_stealing as-is) -
log "restoring concurrency settings to cluster defaults"
set_cluster '{"persistent":{"search.concurrent_segment_search.mode":null,"search.concurrent.max_slice_count":null}}' >/dev/null
log "DONE. Results in $OUTDIR (COMPARISON.md, bench_off.json, bench_on.json, explain_multipartition.json)"
