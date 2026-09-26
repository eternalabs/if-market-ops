# PostgreSQL control worker

Staged at zero replicas until a stopped-EC2 handoff is verified. One replica,
Recreate, AMD64. No PVC: PostgreSQL transactions contain the projection and
Kafka group `if-market.postgres-control.v10` stores the next resume offset for
`clearing.v10`, partition 0. Do not reset or rename this group. A crash after
the DB transaction and before the Kafka commit can retry a record; existing
idempotent projection behavior is retained, not a new exactly-once guarantee.

The image must contain every migration already applied to the database,
including 0016. The first production release must be built from the live
`codex/beta-f-staging` protocol; the older deployment-beta source is not an
approved runtime rollback. Future deployment-beta releases must carry the
current protocol and migration set before updating this worker.

`postgres-worker --check` connects to PostgreSQL, reads the migration version,
and validates the Kafka committed offset against retention. It does not run
migrations, join the group, poll data, write projections or commit offsets.
Use it in a temporary Job before stopping EC2 and again after freezing EC2's
offset. Normal worker startup still validates/applies embedded SQLx migrations.

Pod Identity role `beta-if-market-postgres-worker` is limited to the clearing
topic, the existing group and RDS proxy user `postgres`. The user and database
permissions are preserved from EC2, not broadened. The proxy connection uses
IAM and verify-full TLS with the image's system CA bundle. No static AWS keys
or node-role fallback. The proxy security group admits TCP/5432 from the EKS
node security group; it is not publicly exposed.

Cutover: publish/verify the image and read-only access; stop and disable the EC2
container; confirm its committed offset; activate one Kubernetes replica;
verify startup at that offset and forward durable progress; perform a Recreate
restart and recheck readiness/progress. Remove the service from all beta Compose
overlays and password-refresh targets so it cannot be resurrected. Do not run
the old and new workers together. Rollback requires stopping Kubernetes first.

The EC2 metrics/drain gateway still addresses the old local worker. Its target
must be retargeted separately before relying on whole-platform drain checks;
pod health/progress does not repair that pre-existing monitoring integration.
