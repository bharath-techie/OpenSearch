#!/usr/bin/env python3
"""
Generate match()-augmented variants of the 43 ClickBench PPL queries, to force the
indexed/delegation path (match() can't be answered by pure parquet — it round-trips
to Lucene, which is where work-stealing's collectors live).

Two variants per query → 86 total:
  - NONSEL: AND `match(URL, 'http')`   (~98.8% of docs match → max delegation load)
  - SEL:    AND `match(URL, 'yandex')` (~0.01% of docs match → heavy RG pruning)

Injection rule (preserves each query's shape):
  - If the pipeline's first stage after `source = clickbench` is `where <cond>`,
    rewrite to `where (<cond>) and match(...)`.
  - Otherwise insert `where match(...)` as a new first stage after the source.

Output: ppl_match/<sel|nonsel>/q<i>.ppl  (86 files)
"""
import os, re

PPL_DIR = "/Users/gbh/Documents/dev/OpenSearch/sandbox/qa/analytics-engine-rest/src/test/resources/datasets/clickbench/ppl"
OUT_DIR = "/Users/gbh/Documents/dev/OpenSearch/sandbox/plugins/analytics-backend-datafusion/rust/docs/ppl_match"

VARIANTS = {
    "nonsel": "match(URL, 'http')",    # ~98.8% — maximal delegation work
    "sel":    "match(URL, 'yandex')",  # ~0.01% — heavy row-group pruning
}

# Split a PPL string into its `|`-separated stages, respecting that our queries
# don't contain literal pipes inside strings (verified by inspection of all 43).
def inject(ppl: str, match_expr: str) -> str:
    stages = [s.strip() for s in ppl.split("|")]
    assert stages[0].lower().startswith("source"), ppl
    # stages[1] is the first transform. If it's a `where`, AND into it.
    if len(stages) > 1 and stages[1].lower().startswith("where "):
        cond = stages[1][len("where "):].strip()
        stages[1] = f"where ({cond}) and {match_expr}"
    else:
        stages.insert(1, f"where {match_expr}")
    return " | ".join(stages)

def main():
    for v in VARIANTS:
        os.makedirs(os.path.join(OUT_DIR, v), exist_ok=True)
    n = 0
    for i in range(1, 44):
        src = open(os.path.join(PPL_DIR, f"q{i}.ppl")).read().strip()
        for v, mexpr in VARIANTS.items():
            out = inject(src, mexpr)
            open(os.path.join(OUT_DIR, v, f"q{i}.ppl"), "w").write(out + "\n")
            n += 1
    print(f"wrote {n} files under {OUT_DIR}/{{{','.join(VARIANTS)}}}/")
    # Show a couple of examples for sanity.
    for ex in ("q1", "q22", "q37"):
        print(f"\n-- {ex} --")
        for v in VARIANTS:
            print(f"  [{v}] {open(os.path.join(OUT_DIR, v, ex + '.ppl')).read().strip()}")

if __name__ == "__main__":
    main()
