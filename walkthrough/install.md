# Install

Install for the **beta-if-market** EKS cluster in `us-east-1`.

```bash
aws eks update-kubeconfig --region us-east-1 --name beta-if-market
```

## Storage

`ebs.yml` is the default encrypted `gp3` StorageClass. Auto Mode's provisioner is `ebs.csi.eks.amazonaws.com`. Volume claims stay Pending without this class.

```bash
kubectl apply -f ebs.yml
```

## cert-manager

The ClickHouse operator webhooks need cert-manager.

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager
kubectl -n cert-manager rollout status deploy/cert-manager-webhook
kubectl -n cert-manager rollout status deploy/cert-manager-cainjector
```

## ClickHouse operator

Official operator from ClickHouse/clickhouse-operator. Apply it after the cert-manager webhook pod is Ready.

```bash
kubectl apply --server-side --force-conflicts -f https://github.com/ClickHouse/clickhouse-operator/releases/latest/download/clickhouse-operator.yaml
kubectl -n clickhouse-operator-system rollout status deploy/clickhouse-operator-controller-manager
```

If the webhook objects fail with `no endpoints available for service "cert-manager-webhook"`, run the operator apply again once that pod is Ready.

## ClickHouse

`clickhouse/deployment.yml` creates namespace `clickhouse`, Keeper named `keeper` (3 replicas, 100m/256Mi request, 500m/512Mi limit, 10Gi gp3), and ClickHouse named `clickhouse` (1 shard, 2 replicas, 2 CPU and 8Gi request and limit, 20Gi gp3 each). Images are pinned to `clickhouse/clickhouse-server:25.3` and `clickhouse/clickhouse-keeper:25.3` so the Rust `clickhouse 0.15` worker client can decompress HTTP responses. Do not float `:latest` — 26.x returns LZ4 frames that crash the worker with `decompression error: incorrect magic number`. The operator owns the replica count. Downgrading an existing volume from 26.x to 25.3 is not supported; wipe the `clickhouse` PVCs and let the operator recreate empty disks.

## Argo CD

Argo CD syncs every top-level directory except `argocd/` and `walkthrough/`. `clickhouse/` is plain YAML. `clickhouse-worker/`, `market-maker/` and `prediction-maker/` carry a `kustomization.yaml`; Argo detects it and runs `kustomize build`, and CI in `if-market-rs` rewrites the `images:` tag there on every release.

Push `main` to https://github.com/eternalabs/if-market-ops.git before applying the ApplicationSet. Argo reads that remote, not the files on your laptop.

```bash
kubectl create namespace argocd
kubectl apply --server-side --force-conflicts -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl -n argocd rollout status deploy/argocd-server
kubectl apply -f https://raw.githubusercontent.com/eternalabs/if-market-ops/main/argocd/applicationset.yml
kubectl -n argocd get applications
```

## Sealed Secrets

Secrets are committed as `SealedSecret` objects. `kubeseal` encrypts each value with the controller's public key; only the controller in this cluster can decrypt them. Plaintext values live in the password manager (and, until cutover, in `/etc/ifmarket/market-maker.env` and `.runtime/deployed-bots/secrets/` on the EC2 host). Never commit a raw `Secret`.

```bash
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.27.3/controller.yaml
kubectl -n kube-system rollout status deploy/sealed-secrets-controller
brew install kubeseal   # or the release binary matching the controller version
```

### Master key backup (do this once, immediately)

Every `SealedSecret` in this repo is bound to the controller keypair. Lose it and every secret must be re-sealed from plaintext. Back it up out of band; the same file restores it on a replacement cluster.

```bash
kubectl -n kube-system get secret -l sealedsecrets.bitnami.com/sealed-secrets-key -o yaml \
  > sealed-secrets-master-key.yml
aws secretsmanager create-secret \
  --name beta-if-market/sealed-secrets-master-key \
  --secret-string file://sealed-secrets-master-key.yml
rm -P sealed-secrets-master-key.yml
```

The controller adds a new key every 30 days and keeps the old ones. Re-run the backup (with `put-secret-value`) after a rotation; the label selector captures all keys.

### Restore on a new cluster

Apply the key *before* the controller starts so it is adopted instead of a fresh one being generated.

```bash
aws secretsmanager get-secret-value --secret-id beta-if-market/sealed-secrets-master-key \
  --query SecretString --output text > key.yml
kubectl apply -f key.yml
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.27.3/controller.yaml
kubectl -n kube-system delete pod -l name=sealed-secrets-controller
rm -P key.yml
```

### Sealing a secret

Sealing is strict-scoped: the ciphertext is bound to the Secret name and namespace, so use exactly the names referenced by the Deployments. Each `*.sealed-secret.yml` in this repo starts as a placeholder with the exact command in its header. The general shape:

```bash
kubectl create secret generic market-maker-midterms-btc \
  --namespace market-maker \
  --from-literal=IF_MARKET_API_KEY="$API_KEY" \
  --from-literal=IF_MARKET_WALLET_PRIVATE_KEY="$WALLET_KEY" \
  --dry-run=client -o yaml \
| kubeseal --controller-namespace kube-system --format yaml \
  > market-maker/midterms-btc.sealed-secret.yml
```

Rotate one key without re-entering the rest:

```bash
echo -n "$NEW_VALUE" | kubeseal --raw --controller-namespace kube-system \
  --namespace market-maker --name market-maker-midterms-btc
# paste the output over that key under spec.encryptedData
```

To seal from a machine without cluster access, export the public cert once with `kubeseal --fetch-cert > sealed-secrets.pem` and pass `--cert sealed-secrets.pem`.

## Bot wallets: one per market

On EC2 every market-maker shared one wallet **and** one runtime directory (`.runtime/deployed-bots/market-makers`). The shared directory is what let five processes coordinate: `user-<id>-sequence.json` hands out monotonic command sequences and `user-<id>-mutations.lock` enforces the per-account request budget across processes. Kubernetes pods do not share that directory, so two pods on the same wallet would race sequences and blow the 600 actions/min account budget.

Give each `market-maker/<market>` and `prediction-maker/<market>` its own funded wallet and API key before sealing its secret. Generate credentials with the existing `scripts/gen-bot-jwt-keys.sh` / `scripts/ec2-beta-bots.sh` flow in `if-market-rs`, fund the wallet, then seal. The per-market Secret names are in each `<market>.sealed-secret.yml` header.

## Docker Hub

Images are `docker.io/labseterna/if-market-beta-{clickhouse-worker,market-maker,prediction-maker}:sha-<7>`. If the repositories are private, create a pull secret in each of the three namespaces and add it to the Deployments' `imagePullSecrets` (seal it like any other secret):

```bash
kubectl create secret docker-registry dockerhub-pull \
  --namespace market-maker \
  --docker-username="$DOCKERHUB_USERNAME" --docker-password="$DOCKERHUB_TOKEN" \
  --dry-run=client -o yaml | kubeseal --controller-namespace kube-system --format yaml \
  > market-maker/dockerhub-pull.sealed-secret.yml
```

## GitHub Actions secrets (repo `eternalabs/if-market-rs`)

| Secret | Purpose |
| --- | --- |
| `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN` | `DOCKERHUB_USERNAME` is the user (`eternalabs`), not the org. The token is that user's personal access token with Read & Write. Images are pushed to `docker.io/labseterna/*`. |
| `OPS_REPO_TOKEN` | fine-grained PAT, repository `eternalabs/if-market-ops`, permission Contents: Read and write; the workflow commits `Deploy <services> sha-xxxxxxx` to `main` |

One workflow per service (`.github/workflows/clickhouse-worker.yml`, `market-maker.yml`, `prediction-maker.yml`) triggers on `deployment-beta` when that service's source paths change and calls the shared `_build-image.yml`, which builds `docker/<service>.Dockerfile`, pushes the image and runs `kustomize edit set image` in the matching directory here. Those Dockerfiles are standalone (no Aeron stage); the root `Dockerfile` is still the compose/EC2 build. Each service lands as its own `Deploy <service> sha-xxxxxxx` commit on `if-market-ops` `main`. Open a PR into `deployment-beta` to release; do not push that branch directly. Run a service's workflow manually from the Actions tab (`workflow_dispatch`) to force a build.

## ClickHouse worker

### ClickHouse user

The worker authenticates with `CLICKHOUSE_WORKER_CLICKHOUSE_USER` / `_PASSWORD` from `clickhouse-worker/sealed-secret.yml`. `bootstrap-user-job.yml` creates that user on every Argo sync (hook-weight 5) and grants `SELECT` on `system.tables`, `system.columns`, and `system.mutations` — startup validation reads those catalogs. To create it by hand:

```bash
kubectl -n clickhouse exec -it clickhouse-clickhouse-0-0-0 -- clickhouse-client --query "
  CREATE USER IF NOT EXISTS ifmarket IDENTIFIED WITH sha256_password BY '<password>';
  CREATE DATABASE IF NOT EXISTS if_market_logs;
  GRANT ALL ON if_market_logs.* TO ifmarket;
  GRANT SELECT ON system.tables TO ifmarket;
  GRANT SELECT ON system.columns TO ifmarket;
  GRANT SELECT ON system.mutations TO ifmarket;
"
```

The operator only created the headless Service `clickhouse-clickhouse-headless`. `CLICKHOUSE_WORKER_CLICKHOUSE_URL` is pinned to replica `clickhouse-clickhouse-0-0-0` so reads and writes hit the same MergeTree copy.

### Worker TOML

`clickhouse-worker/clickhouse-worker.toml` mirrors `crates/clickhouse-worker/clickhouse-worker.toml` minus anything connection-related: brokers, user and password are placeholders that the binary replaces from `CLICKHOUSE_WORKER_*` environment variables before validating, so the committed file is credential-free. When the upstream file changes table names, stream ids or batching limits, copy those sections here; leave the placeholders.

### Schema

`clickhouse-worker/0001_init.sql` is a copy of `crates/clickhouse-worker/migrations/0001_init.sql`. `schema-init-job.yml` applies it as an Argo `PreSync` hook on every sync; every statement is `IF NOT EXISTS`, so re-runs are no-ops. When the schema changes in `if-market-rs`, copy the file here in the same release.

**Replication caveat.** `clickhouse/deployment.yml` runs 1 shard x 2 replicas, but the schema uses non-replicated `MergeTree` engines. Inserts land only on whichever replica the Service routes to, and reads from the other replica see nothing. Until the schema moves to `ReplicatedMergeTree`, either set `replicas: 1` in `clickhouse/deployment.yml` or point `CLICKHOUSE_WORKER_CLICKHOUSE_URL` at a single replica's per-pod Service. Decide this before the first insert.

### MSK IAM (EKS Pod Identity)

The worker reads MSK with IAM auth. Done on 2026-09-25 for `beta-if-market`:

- Addon `eks-pod-identity-agent` enabled on the cluster.
- IAM role `beta-if-market-clickhouse-worker` (trust: `pods.eks.amazonaws.com`) with inline policy `msk-read`: `kafka-cluster:Connect`, `DescribeCluster` on cluster `beta-if-market-kafka`; `DescribeTopic`, `ReadData` on topics `clearing.v10`, `realtime.spot.v1`, `realtime.candles.v1`; `DescribeGroup`, `AlterGroup` on groups `if-market.clickhouse-worker.k8s*`. Read-only: it cannot write or create topics.
- Pod Identity association `a-dgc8wleooymwssum6`: namespace `clickhouse-worker`, service account `clickhouse-worker`.

The manifest needs no change. To recreate on a new cluster:

```bash
aws eks create-addon --region us-east-1 --cluster-name <cluster> --addon-name eks-pod-identity-agent
aws eks create-pod-identity-association --region us-east-1 --cluster-name <cluster> \
  --namespace clickhouse-worker --service-account clickhouse-worker \
  --role-arn arn:aws:iam::533267424142:role/beta-if-market-clickhouse-worker
```

The MSK brokers' security group must admit the EKS pod CIDR on 9098; if the worker logs connection timeouts, that is the first thing to check.

Credentials created the same day and stored in AWS Secrets Manager (`us-east-1`): `beta-if-market/sealed-secrets-master-key` and `beta-if-market/clickhouse-ifmarket-password`.

## Cutover order

1. Sealed Secrets controller installed, master key backed up.
2. ClickHouse user created; `clickhouse-worker/common-env.yml` URL verified; replication decision made.
3. Seal every placeholder `*.sealed-secret.yml` (11 files) and commit. Argo creates the namespaces; bot pods will stay in `Pending`/`CreateContainerConfigError` only for secrets that are still placeholders.
4. First CI run on `deployment-beta` (or `workflow_dispatch`) pushes the three images and commits real tags here; Argo rolls them out.
5. `clickhouse-worker`: it runs on its own consumer group (`if-market.clickhouse-worker.k8s.manual`), so it can consume alongside the EC2 worker writing to ClickHouse Cloud. Compare row counts, then stop the EC2 worker and decide where the API reads from.
6. Bots, one market at a time: `docker compose stop market-maker-<x>` on EC2, confirm its resting orders are cancelled, then `kubectl -n market-maker scale deploy/market-maker-<x> --replicas=1` (all bot Deployments can start at `replicas: 0` by editing the file before step 3 if you want that gate). Never run one market's bot in both places at once.

## S3 archive worker

`s3-worker/` is staged at zero replicas with its own EKS Pod Identity and sealed
broker/registry credentials. The EC2 S3 worker remains the archive writer.
The new `if-market-rs` s3-worker image workflow follows the same deployment-beta
build/tag-bump pattern as ClickHouse; publishing an image does not activate it.
No PVC is needed: this worker recovers from S3 segments, indexes and route tails.
Its staged IAM role permits reads, not uploads. Follow
[`s3-worker/README.md`](../s3-worker/README.md) for the separate exclusive-writer
handoff, write-policy activation and all nine stream retention checks. Do not
run both workers against `ifmarket-archive-tape-beta/v2/`.
