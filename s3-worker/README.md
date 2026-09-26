# S3 archive worker — active on Kubernetes

Kubernetes is the sole writer of `s3://ifmarket-archive-tape-beta/v2/`.
The nine-route Deployment runs one replica with Recreate. EC2's S3 worker was
stopped at `2026-09-26T08:45:19Z`, its restart policy disabled, and its service
removed from all beta Compose overlays. Kafka and other EC2 services were not
restarted (all 26 other running container IDs were unchanged). The stopped
S3 container was removed after verification; its image and S3 data were retained.
Do not restore the EC2 service while Kubernetes is active.

Release: `sha-ecf7a82`, image digest
`sha256:391a660f5cf140444fde6eadf7db851fb692f0deaacb8471e4b3c6d53d613890`.
Application release PR: `eternalabs/if-market-rs#9`; GitOps activation: `9372a63`.
The application staging branch includes Compose removal `2f06a44` and test-fixture
correction `26e4b35`. These are deployed in the EC2 checkout without a stack restart.

Both `archive-read` and the narrow `archive-write` inline policies are installed.
Uploads are allowed only under this bucket's `v2/*`; deletion remains denied.

## Verified handoff — 2026-09-26

- The stopped EC2 writer had no pending orphan segments. All nine resume
  positions were in Kafka retention; all eight non-empty tail segments passed
  CRC/index validation and their last payloads matched committed Kafka records.
  Quarantine was empty, with Kafka's retained range also empty at offset zero.
- The published release's read-only `verify-drain` command returned
  `{"verified":true}` under Kubernetes Pod Identity before upload access was granted.
- All nine initial worker startup tails matched the frozen EC2 cutoff exactly.
  All eight non-empty routes subsequently advanced in S3.
- A controlled Recreate restart completed without concurrent old/new pods.
  Its replacement became Ready on all nine routes and continued uploading.
  Shutdown used the configured termination window; it was not an explicit drain.
- A record-for-record audit checked **95,034 committed messages across 30 new
  segments**, from the EC2 cutoff through post-restart snapshots. Ordered Kafka
  offsets, Aeron cursors and payload hashes matched; frame CRCs passed. No missing
  or duplicate committed messages were found within that checked interval.
  Transaction control/aborted records can consume Kafka offsets without appearing
  as data messages; consecutive integer offsets are not the correctness test.
- Post-restart tail segments/indexes passed the same integrity checks. All 18
  offline Compose-wrapper regression tests passed both locally and on EC2;
  the combined EC2 Compose model no longer contains `s3-worker`.

Exact per-route cutoffs, checked ranges and image identity are in
[`checks/cutover-2026-09-26.json`](checks/cutover-2026-09-26.json).
This verifies the handoff interval, not every historical archive object or
long-duration AWS credential refresh behavior.

### Monitoring follow-up

Kubernetes probes and pod scrape annotations are configured. However, EC2's
existing metrics/drain gateway still addresses the removed local
`s3-worker:9100`. Those S3 routes are not yet retargeted to Kubernetes. The
cross-service drain observer therefore cannot certify a full-platform drain
until this integration is updated; do not bypass it or treat it as healthy.
No private/public load balancer, firewall rule or monitoring target was silently
changed during this archive-writer handoff.

## Historical staging checks — 2026-09-26 (before activation)

- Application pipeline commit: `49cd11d` on `codex/beta-f-staging` (not merged
  into deployment-beta). Initial ops deployment commit: `3ff8873`.
- Argo installed the namespace, service account, ConfigMaps, both decrypted
  sealed Secrets and the zero-replica Deployment; application Synced/Healthy.
- The read-only access Job successfully assumed the exact S3-worker role and
  listed all nine route prefixes. It could HEAD all eight existing tail objects;
  the quarantine route is empty and has no tail to check. No S3 writes occurred.
- IAM simulation allowed GetObject and denied PutObject/DeleteObject for the
  configured archive prefix. The activation write policy remains unattached.
- 26 S3-worker unit tests, workflow actionlint, Dockerfile build checks,
  Kubernetes TOML contract checks and server-side manifest dry runs passed.
  Strict clippy is not clean: seven pre-existing style warnings in config.rs,
  segment.rs and worker.rs are promoted to errors by `-D warnings`. The image
  workflow's gate is cargo check, not strict clippy; no runtime code was changed.
- At this staging checkpoint, image publication, Kafka connectivity and a
  nine-route restart/cutover rehearsal were still outstanding. Subsequent
  handoff results are recorded below.

## Architecture and storage

Unlike the ClickHouse worker, this worker needs no PVC. Its existing protocol
uploads a segment, its index and then `tail.json`, and startup reconciles S3
objects before resuming Kafka. S3, not Kafka group commits or local files, is the
resume authority. All nine routes must pass validation for `/ready` to succeed.
Kafka group prefixes are distinct for operational isolation, not writer fencing:
different groups do NOT make simultaneous writes to the same S3 prefix safe.

Keep one replica and Recreate after activation. Never start a replacement while
an old writer may still run, including on an unreachable node. The 90-second
termination grace is not proof of drain: inspect durable tails and segment
recovery explicitly. `/tmp` is disposable scratch. No static AWS keys or node
role fallback are configured. The nine in-memory 64 MiB buffers plus SDK copies
need memory headroom; start with a 1 GiB request / 2 GiB limit and measure before
tuning. Producer/Kafka retention settings are not changed by this deployment.
The release pipeline currently builds only `linux/amd64`, so the Deployment
explicitly selects amd64 nodes in this mixed-architecture cluster.

## Build and GitOps

`docker/s3-worker.Dockerfile` and `.github/workflows/s3-worker.yml` are merged
into `if-market-rs` branch `deployment-beta`. The existing shared image workflow
builds `docker.io/labseterna/if-market-beta-s3-worker:sha-<commit>` and updates this
directory's image tag. It does NOT change replicas or IAM permissions.

The first release image was pulled and executed successfully using the cluster's
registry credentials before EC2 was stopped. Image CI does not change replica
count or IAM permissions.
The broker and registry credentials are sealed separately for this namespace;
copying ClickHouse's namespace-bound ciphertext would not work.

## AWS identity

Role: `arn:aws:iam::533267424142:role/beta-if-market-s3-worker`.
EKS Pod Identity association: `a-flfzoxphiszaups4z`, cluster `beta-if-market`,
namespace/service account `s3-worker` / `s3-worker`.

- `iam/trust.json` restricts trust to that cluster, namespace and service account.
- Installed `archive-read` policy (`iam/read-policy.json`) permits consuming
  exactly the nine configured MSK topics, using `if-market.s3-worker.k8s*` groups,
  and listing/reading only the existing bucket's `v2/` prefix. This policy itself
  grants no topic writes, topic creation, S3 deletion or S3 uploads.
- `archive-write` (`iam/activate-write-policy.json`) was installed only after
  EC2 stopped and both independent and release-binary boundary checks passed.
  It adds `s3:PutObject` for `v2/*`, not bucket administration or deletion.
- The bucket uses SSE-S3 (AES256); KMS permissions are not currently needed.

The reusable `checks/access-job.yml` only checks Pod Identity and S3 list/head
access on all nine routes. It performs no uploads or Kafka consumption. Run it
manually after Argo sync. It is deliberately excluded from kustomization.

## Future handoff / rollback gates — do not bypass

1. Publish the image through the normal reviewed `deployment-beta` release and
   verify config parsing, all nine routes, AWS credential refresh, Kafka IAM
   connectivity and the S3 crash/restart tests. Read-only AWS access checks do
   not establish Kafka connectivity or archive continuity.
2. Stop the EC2 S3 worker and disable its automatic restart/redeployment. Leave
   Kafka and producers alone unless the approved drain plan says otherwise.
3. Capture and verify every route's durable tail, epoch, referenced segment and
   sidecar, including any uploaded objects beyond the tail. Compare resume
   positions to retained Kafka ranges. Keep `allow_gap_resume=false`; a missing
   retained interval blocks handoff instead of silently creating archive holes.
4. Keep the same bucket/prefix/topics. Do not switch to a fresh prefix as a
   workaround for a retention gap. Do not reset consumer groups: this worker
   does not use Kafka commits as its resume authority.
5. Attach the narrow write policy, then change replicas to 1 in Git. Check all
   nine route startup logs, HTTP `/ready`, durable progress and a controlled
   restart before declaring the handoff complete.
6. Rollback also requires a singleton handoff: stop/fence Kubernetes first,
   verify its S3 state, then resume EC2. Never start both to test which wins.
