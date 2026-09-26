# Kubernetes worker preparation (not a data cutover)

The image tag remains CI-managed and clearing mode remains explicitly `legacy`.
This preparation does not copy Cloud history, reset Kafka offsets, bootstrap
consumer groups, drop tables, or redirect the API/EC2 writer.

## Verified deployment — 2026-09-26

- Preparation commit: `ffc8402`, synced by Argo; schema/bootstrap jobs succeeded.
- PVC `clickhouse-worker/clickhouse-worker-journal` is Bound to PV
  `pvc-99d9ffa8-a249-4351-9749-d1d1e039508c` with reclaim policy Retain.
- AWS confirms `vol-02cafddfef1099bcf`: encrypted gp3, 2 GiB, us-east-1b.
- UID/GID 65532 could write the mount. A temporary proof file survived a
  controlled Recreate restart from pod `clickhouse-worker-595b988dd-kzmjn` to
  `clickhouse-worker-b977f6976-dzltf`; the test file was then removed.
- Both existing volume aggregate inner tables now report
  `non_replicated_deduplication_window = 4096`.
- Image stayed `sha-612f056`, mode stayed legacy, Kubernetes group prefix stayed
  distinct, and no clearing journals were bootstrapped or Kafka offsets reset.
- The replacement pod runs without container restarts but reports unready (HTTP
  503): legacy clearing is empty while Kafka's retained low is 39,696,907. This
  predates preparation; the new probe exposes it correctly. Storage preparation
  is complete, but ingestion activation and a groups recovery rehearsal are not.

## Persistent recovery storage

`journal-storage.yml` creates an encrypted 2 GiB gp3 claim through EKS Auto Mode.
Its dedicated StorageClass uses `Retain`, not the cluster default's `Delete`.
The PV/EBS volume is dynamically provisioned when a consuming pod is scheduled.
The claim and StorageClass have Argo prune/delete protection. Keep the same PVC
on every rollout. Losing the claim or restoring an old snapshot is a recovery
incident, not permission to recreate checkpoints or reset Kafka offsets.

The mount at `/var/lib/ifmarket-clickhouse-worker` is writable through fsGroup
65532. Four clearing journals each have a 128 MiB cap; 2 GiB accommodates their
atomic-write temporary files. This is bounded recovery state, not Kafka history.
No empty/placeholder journal files are created during preparation. The actual
bootstrap command creates validated journals at the later cutover.

Keep one replica and Recreate. ReadWriteOnce is node-level attachment control,
not a cross-host writer fencing protocol. Never force-delete an unreachable
writer and assume it stopped. The local journal lock adds same-volume process
exclusion once groups mode is active. EBS is zonal; recovery onto another AZ is
not automatic. Retain preserves the disk, not a continuously consistent backup
of the disk, ClickHouse and Kafka together.

## Schema and health

Schema init adds insert deduplication to newly created volume aggregate targets.
`prepare-groups.sh` resolves the two existing materialized-view inner tables and
raises their non-replicated deduplication windows to at least 4096. It preserves
all rows and larger pre-existing settings. The worker also needs SELECT on
system.merge_tree_settings for its future groups preflight.

Readiness uses HTTP `/ready`, not a listening TCP port. Until the missing
clearing history is restored, legacy mode reports unready; this is expected and
must not be hidden by changing the probe back. Startup/liveness remain TCP so a
data-baseline problem does not create a restart loop. Argo can show Progressing
or Degraded while preparation is installed; that does not certify ingestion.

## Gates before activation

1. Finish the separately authorized Cloud-to-Kubernetes data cutover design.
   Cloud's legacy batch/checkpoint tables have already been retired: the old
   staging-table bootstrap runbook cannot simply be reused. Implement and test
   a destination-baseline bootstrap using verified source group checkpoints
   and all matching business/state tables. Never copy Cloud offsets alone.
2. Stop/fence writers, record consistent source checkpoints and copy/validate
   all destination tables, including ingest_open_orders and aggregate handling.
   Reconcile the existing partial Kubernetes spot/candle history explicitly.
3. Ship the groups-capable image; retain distinct Kubernetes group identities.
   Bootstrap its journals on this PVC, as UID/GID 65532, only after the verified
   baseline is present. Journal identity includes destination and group details:
   copying the EC2 journal files unchanged is invalid.
4. Make schema init mode-aware before retiring Kubernetes legacy tables. The
   current legacy schema job intentionally still creates those tables.
5. Recheck Kafka retention for every stream and all source/MV dedup settings.
   The cluster is on ClickHouse 25.3; run the recovery tests against that exact
   version (including dependent-view failures), not only the newer Cloud server.
   Size windows for actual insert block counts and downtime; the current worker
   refuses ambiguous retries beyond its configured safety deadline.
6. Activate groups, verify HTTP readiness and per-group progress, and rehearse
   interrupted writes and pod replacement with persistent recovery state. A
   mount persistence test alone is not a groups crash-recovery rehearsal.
7. Switch API traffic only after history and live progress are verified. No
   concurrent writers may use the same sink/group with independent journals.
