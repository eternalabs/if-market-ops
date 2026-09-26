# S3 archive worker — Kubernetes staging

This deploys the nine-route Kafka-to-S3 worker configuration, NOT a second active
archive writer. `replicas: 0` and a read-only AWS policy are deliberate gates.
EC2 `ifmarket-s3-worker` remains the sole writer of
`s3://ifmarket-archive-tape-beta/v2/` until a separately verified handoff.

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

## Build and GitOps

`if-market-rs` branch `codex/beta-f-staging` adds
`docker/s3-worker.Dockerfile` and `.github/workflows/s3-worker.yml`.
After review/merge into `deployment-beta`, the existing shared image workflow
builds `docker.io/labseterna/if-market-beta-s3-worker:sha-<commit>` and updates this
directory's image tag. It does NOT change replicas or IAM permissions.

`awaiting-first-build` is intentionally not a runnable tag. Do not enable the
Deployment before a successful image build/push and an exact tag/digest check.
No application-branch merge is performed as part of infrastructure staging.
The broker and registry credentials are sealed separately for this namespace;
copying ClickHouse's namespace-bound ciphertext would not work.

## AWS identity

Role: `arn:aws:iam::533267424142:role/beta-if-market-s3-worker`.
EKS Pod Identity association: `a-flfzoxphiszaups4z`, cluster `beta-if-market`,
namespace/service account `s3-worker` / `s3-worker`.

- `iam/trust.json` restricts trust to that cluster, namespace and service account.
- Installed `archive-read` policy (`iam/read-policy.json`) permits consuming
  exactly the nine configured MSK topics, using `if-market.s3-worker.k8s*` groups,
  and listing/reading only the existing bucket's `v2/` prefix. No topic writes,
  topic creation, S3 deletion or S3 uploads are granted.
- `iam/activate-write-policy.json` is NOT installed during staging. Only apply
  it after the EC2 writer has stopped and the exclusive handoff is authorized.
  It adds `s3:PutObject` for `v2/*`, not bucket administration or deletion.
- The bucket uses SSE-S3 (AES256); KMS permissions are not currently needed.

The reusable `checks/access-job.yml` only checks Pod Identity and S3 list/head
access on all nine routes. It performs no uploads or Kafka consumption. Run it
manually after Argo sync. It is deliberately excluded from kustomization.

## Activation gates — do not bypass

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
