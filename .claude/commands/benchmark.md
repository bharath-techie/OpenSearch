# OpenSearch DataFusion Benchmark

Run the deterministic benchmark scripts. Your role is to help the user pick the right options and invoke the scripts — not to SSH around manually.

## Scripts

- `scripts/deploy-ec2.sh` — Infrastructure + deployment
- `scripts/benchmark-ec2.sh` — Benchmark execution

## Workflow

1. Ask user what they want to do: setup infra, deploy code, run benchmark, check status
2. Build the appropriate command with their options
3. Run it and report results

## Examples

```bash
# First time: provision instances
./scripts/deploy-ec2.sh setup

# Deploy current code to remote
./scripts/deploy-ec2.sh deploy

# Quick re-deploy after a code change (no native rebuild)
./scripts/deploy-ec2.sh redeploy

# Run benchmark: 10% data, all working queries, concurrency ladder
./scripts/benchmark-ec2.sh run --pct 10 --queries all --concurrency "4 8 16 32"

# Just the heavy queries at high concurrency
./scripts/benchmark-ec2.sh run --pct 10 --queries heavy --concurrency "4 8 16 32 64" --skip-ingest

# Check progress
./scripts/benchmark-ec2.sh status

# Get results
./scripts/benchmark-ec2.sh results

# Upload to S3
./scripts/benchmark-ec2.sh upload my-bucket
```

## When to intervene manually

Only help debug if a script fails. Common issues:
- AWS credentials expired → `ada credentials update --account=<ID> --role=Admin --provider=isengard --once`
- Security group mismatch → both instances must be in same SG
- Stale build files → `deploy-ec2.sh deploy` uses `rsync --delete`
- Missing PPL endpoint → ensure `test-ppl-frontend` is in the plugin list
