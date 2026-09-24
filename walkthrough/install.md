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

`clickhouse/deployment.yml` creates namespace `clickhouse`, Keeper named `keeper` (3 replicas, 100m/256Mi request, 500m/512Mi limit, 10Gi gp3), and ClickHouse named `clickhouse` (1 shard, 2 replicas, 2 CPU and 8Gi request and limit, 10Gi gp3 each). The operator owns the replica count.

## Argo CD

Argo CD syncs every top-level directory except `argocd/` and `walkthrough/`. `clickhouse/` is the first one. A later service is another directory with a `deployment.yml`, for example `bots/deployment.yml` or `s3-worker/deployment.yml`.

Push `main` to https://github.com/eternalabs/if-market-ops.git before applying the ApplicationSet. Argo reads that remote, not the files on your laptop.

```bash
kubectl create namespace argocd
kubectl apply --server-side --force-conflicts -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl -n argocd rollout status deploy/argocd-server
kubectl apply -f https://raw.githubusercontent.com/eternalabs/if-market-ops/main/argocd/applicationset.yml
kubectl -n argocd get applications
```
