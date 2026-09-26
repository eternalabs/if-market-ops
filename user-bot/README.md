# Kubernetes user bot

This service packages the existing single-process **user-flow-bot**, not the
legacy per-profile user-bot. The image repository is
`labseterna/if-market-beta-user-bot`; Dockerfile/workflow use the name user-bot.
One process on account 11 discovers every configured open market. Never scale
beyond one or run it concurrently with EC2's `ifm-user-flow`.

Initially staged at zero replicas. Keep Recreate and the AMD64 node selector.
The mounted TOML preserves the live beta trade sizes, four impact assets,
inventory caps, shock settings and merge behavior. Only API/WebSocket URLs and
the absolute runtime path change for Kubernetes. Credentials remain the same
account and are namespace-scoped SealedSecrets. No AWS identity is needed.

The 1 GiB gp3 PVC retains command sequences across pod replacement and is
protected against automatic Argo pruning. After stopping EC2, copy only the
account's durable sequence JSON to this volume with uid/gid 65532, not stale
process lock files. Exchange positions/open orders are recovered from realtime;
the existing startup reconciliation and inherited-order cancellation remain
unchanged. A fresh process does not preserve in-memory random scheduling or
shock timers; existing recovery drains inherited impact inventory first.

Run the image with `--config /app/user-flow.toml --check` before activation.
This authenticates, validates the wallet and catalog and reads private snapshots,
but places/cancels/merges no orders. Use a separate scratch runtime for this
preflight while EC2 is active. Freeze/copy the production sequence only after
EC2 is stopped and its restart policy disabled.

The TCP readiness probe proves the metrics listener is alive, not that trading
feeds are fresh. Verify `user flow realtime state ready`, actual account order/
fill activity, credential errors and liquidity-guard messages after startup.
Do not weaken liquidity guards to make a rollout look active. EC2's legacy
launcher is retired on the migration branch to prevent accidental duplicates.
