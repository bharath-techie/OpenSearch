#!/usr/bin/env python3
"""ClickBench OFF-vs-ON harness: rigorous correctness + performance, single-partition.

Compares the route_pure_parquet_through_indexed flag OFF (vanilla) vs ON (indexed) across
all 43 ClickBench PPL queries. Loads queries from cb_queries.json (sibling file).

Usage:
  # single-partition A/B requires BOTH (see route-pure-parquet-perf-handoff.md §2):
  curl -XPUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' \
    -d '{"persistent":{"search.concurrent_segment_search.mode":"none","datafusion.min_target_partitions":1}}'
  python3 cb_harness.py
  # writes /tmp/cb_correctness.json and /tmp/cb_perf.json; resets flag is NOT automatic.
"""
import os, json, time, urllib.request, urllib.error, sys, statistics

BASE = "http://localhost:9200"
QUERIES = dict(json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cb_queries.json"))))
ORDER = [n for n, _ in json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cb_queries.json")))]
TIMEOUT = 300

# ---- HTTP ----
def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, r.read(), (time.time()-t0)*1000
    except urllib.error.HTTPError as e:
        return e.code, e.read(), (time.time()-t0)*1000
    except Exception as e:
        return 0, str(e).encode(), (time.time()-t0)*1000

def set_flag(val):
    # Set persistent AND null any transient override. OpenSearch precedence is
    # transient > persistent, so a leftover transient route flag (e.g. from a
    # manual A/B) silently pins every query to vanilla and makes the whole run
    # vanilla-vs-vanilla. Nulling transient here guarantees `val` is effective.
    KEY="datafusion.indexed.route_pure_parquet_through_indexed"
    s,_,_ = http("PUT","/_cluster/settings",
        {"persistent":{KEY:val},"transient":{KEY:None}})
    assert s==200, "set flag failed"
    # Read back the EFFECTIVE value and assert it matches; fail loud otherwise.
    _,b,_ = http("GET","/_cluster/settings?flat_settings=true")
    d=json.loads(b)
    eff = d.get("transient",{}).get(KEY, d.get("persistent",{}).get(KEY))
    assert str(eff).lower()==str(val).lower(), f"route flag not effective: wanted {val}, got {eff}"

def clear_caches():
    http("POST","/clickbench/_cache/clear",None); time.sleep(0.3)

def ppl(sql, retries=5):
    """Run a PPL query. Retry on 429 (admission control), exp backoff."""
    last=None
    for i in range(retries):
        s,b,dt = http("POST","/_plugins/_ppl",{"query":sql})
        if s==429:
            last=(s,b,dt); time.sleep(1.5*(i+1)); continue
        rows=None; err=None
        try:
            d=json.loads(b)
            if "datarows" in d: rows=d["datarows"]
            else: err=str((d.get("error",{}) or {}).get("reason") or d.get("error") or d)[:200]
        except Exception:
            err=b[:200].decode("utf-8","replace")
        return {"status":s,"ms":round(dt,1),"rows":rows,"err":err}
    s,b,dt=last
    return {"status":s,"ms":round(dt,1),"rows":None,"err":"429 after retries"}

# ---- deterministic variants for correctness ----
import re
# Raw-row queries: total-order sort with tiebreaker, keep head.
DET_OVERRIDE = {
    "q24-google-urls-sorted": "source = clickbench | where like(URL, '%google%') | sort EventTime, WatchID | fields EventTime, WatchID, URL | head 10",
    "q25-search-phrases-by-time": "source = clickbench | where SearchPhrase != '' | sort EventTime, WatchID | fields EventTime, WatchID, SearchPhrase | head 10",
    "q20-specific-user": "source = clickbench | where UserID = 435090932899640449 | fields UserID",
    "q26-search-phrases-sorted": "source = clickbench | where SearchPhrase != '' | sort SearchPhrase, WatchID | fields SearchPhrase, WatchID | head 10",
    "q27-search-phrases-multi-sort": "source = clickbench | where SearchPhrase != '' | sort EventTime, SearchPhrase, WatchID | fields EventTime, SearchPhrase, WatchID | head 10",
}
# scalar single-row aggregates (compare directly)
SCALAR = {"q01-count-all","q02-count-adv-engine","q03-sum-count-avg","q04-avg-userid",
          "q05-distinct-userid","q06-distinct-searchphrase","q07-min-max-eventdate",
          "q21-google-urls","q30-resolution-width-sums"}
MALFORMED = {"q29-referer-analysis"}  # missing `where` in source workload

def parse_group_keys(sql):
    """Extract the group keys after the LAST 'stats ... by <keys>' (before next |)."""
    m=None
    for mm in re.finditer(r'\bby\b\s+(.+?)(?:\||$)', sql): m=mm
    if not m: return []
    keys=[k.strip().strip('`') for k in m.group(1).split(",")]
    return [k for k in keys if k]

def det_groupby(name):
    """For a group-by head-N query, build:
       (A) deterministic top-N: original sort + group keys as tiebreakers, keep head;
       (B) group-count: count total groups (truncation-immune)."""
    sql=QUERIES[name]
    segs=[s.strip() for s in sql.split("|")]
    keys=parse_group_keys(sql)
    # locate sort + head segments
    sort_i=next((i for i,s in enumerate(segs) if s.startswith("sort")),None)
    head_seg=next((s for s in segs if s.startswith("head")),None)
    if sort_i is None or not keys:
        return None,None
    tiebreak=", "+", ".join(keys)
    segs[sort_i]=segs[sort_i]+tiebreak
    topn=" | ".join(s for s in segs if not s.startswith("head"))
    if head_seg: topn+=" | "+head_seg
    # group count: take everything up to & incl the stats, then count groups
    stats_i=next((i for i,s in enumerate(segs) if s.startswith("stats")),None)
    cnt=" | ".join(segs[:stats_i+1])+" | stats count() as __groups"
    return topn,cnt

def canon(rows):
    return sorted(json.dumps(r,sort_keys=True,default=str) for r in rows)

# ---- correctness pass ----
def both(sql):
    set_flag(False); off=ppl(sql)
    set_flag(True);  on =ppl(sql)
    return off,on

def correctness():
    print("\n"+"="*78+"\nCORRECTNESS (deterministic, exact match required)\n"
          "  scalar/raw: exact ordered rows | group-by: top-N exact + total group-count exact\n"+"="*78,flush=True)
    out={}
    for name in ORDER:
        if name in MALFORMED:
            print(f"{name:32s} SKIP (malformed in source workload)",flush=True)
            out[name]={"verdict":"SKIP-malformed"}; continue

        if name in DET_OVERRIDE or name in SCALAR:
            dq=DET_OVERRIDE.get(name,QUERIES[name])
            off,on=both(dq)
            if off["rows"] is None or on["rows"] is None:
                v=f"ERROR OFF={off['status']} ON={on['status']} ({off['err'] or on['err']})"
            else:
                v="EXACT-MATCH (%d rows)"%len(off["rows"]) if off["rows"]==on["rows"] else "*** MISMATCH (ordered rows) ***"
            out[name]={"verdict":v,"mode":"exact","det_sql":dq}
            print(f"{name:32s} {v}",flush=True); continue

        # group-by: deterministic top-N + total group count
        topn,cnt=det_groupby(name)
        if topn is None:
            off,on=both(QUERIES[name])
            v=("EXACT-MATCH (%d rows)"%len(off["rows"]) if (off["rows"] is not None and off["rows"]==on["rows"])
               else f"REVIEW (no group keys parsed) OFF={off['status']} ON={on['status']}")
            out[name]={"verdict":v,"mode":"raw"}; print(f"{name:32s} {v}",flush=True); continue
        o1,n1=both(topn)        # top-N rows
        o2,n2=both(cnt)         # total group count
        parts=[]
        if o1["rows"] is None or n1["rows"] is None:
            parts.append(f"topN ERROR OFF={o1['status']} ON={n1['status']} ({o1['err'] or n1['err']})")
        else:
            parts.append("topN EXACT(%d)"%len(o1["rows"]) if o1["rows"]==n1["rows"] else "*** topN MISMATCH ***")
        if o2["rows"] is None or n2["rows"] is None:
            parts.append(f"grpcount ERROR OFF={o2['status']} ON={n2['status']}")
        else:
            parts.append("grpcount EXACT(%s)"%o2["rows"][0][0] if o2["rows"]==n2["rows"]
                         else "*** grpcount MISMATCH OFF=%s ON=%s ***"%(o2["rows"],n2["rows"]))
        v=" | ".join(parts)
        out[name]={"verdict":v,"mode":"groupby","topn_sql":topn,"cnt_sql":cnt}
        print(f"{name:32s} {v}",flush=True)
    json.dump(out,open("/tmp/cb_correctness.json","w"),indent=2)
    return out

# ---- performance pass ----
def perf(iters=3):
    print("\n"+"="*78+"\nPERFORMANCE (original queries; cold=cache-cleared, hot=median of %d)\n"%iters+"="*78,flush=True)
    print(f"{'query':32s} {'OFF cold':>9} {'OFF hot':>9} {'ON cold':>9} {'ON hot':>9} {'hot Δ%':>7}",flush=True)
    out={}
    for name in ORDER:
        sql=QUERIES[name]
        row={}
        for label,val in (("off",False),("on",True)):
            set_flag(val)
            clear_caches()
            cold=ppl(sql)
            hots=[ppl(sql)["ms"] for _ in range(iters)]
            row[label]={"cold":cold["ms"],"hot":round(statistics.median(hots),1),
                        "status":cold["status"],"n":None if cold["rows"] is None else len(cold["rows"])}
        oc,oh=row["off"]["cold"],row["off"]["hot"]; nc,nh=row["on"]["cold"],row["on"]["hot"]
        delta = round((nh-oh)/oh*100,1) if (row["off"]["status"]==200 and row["on"]["status"]==200 and oh>0) else None
        out[name]=row
        ds = f"{delta:+.1f}" if delta is not None else "  -  "
        flag = ""
        if row["off"]["status"]!=200 or row["on"]["status"]!=200:
            flag=f"  [OFF={row['off']['status']} ON={row['on']['status']}]"
        print(f"{name:32s} {oc:9.1f} {oh:9.1f} {nc:9.1f} {nh:9.1f} {ds:>7}{flag}",flush=True)
    json.dump(out,open("/tmp/cb_perf.json","w"),indent=2)
    return out

if __name__=="__main__":
    # assert single partition
    s,b,_=http("GET","/_cluster/settings?include_defaults=true&flat_settings=true")
    mode=[v for sect in json.loads(b).values() for k,v in sect.items() if k=="search.concurrent_segment_search.mode"]
    print("concurrent_segment_search.mode =",mode,"(none => target_partitions=1)",flush=True)
    correctness()
    perf(iters=3)
    print("\nDONE",flush=True)
