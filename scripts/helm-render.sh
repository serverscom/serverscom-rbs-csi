#!/usr/bin/env bash
# Renders a single install manifest from the Helm chart.
#
# Requires: helm >= 3, yq >= 4.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHART="$ROOT/charts/serverscom-rbs-csi"
OUTPUT="${ROOT}/rbs-csi-deploy.yaml"

TMP="$(mktemp -d "${RUNNER_TEMP:-$HOME}/render-deploy.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

helm template rbs-csi "$CHART" \
  --namespace kube-system \
  --set-string "controller.driver.image.tag=${TAG}" \
  --set-string "node.driver.image.tag=${TAG}" \
  --set-string "api.token=dummy" \
  --output-dir "$TMP/rendered" >/dev/null

SRC="$TMP/rendered/serverscom-rbs-csi/templates"

# Strip labels/annotations.
# app.kubernetes.io/name is kept intentionally — it's a standard, useful label.
clean() {
  yq ea '
    del(
      .metadata.labels."helm.sh/chart",
      .metadata.labels."app.kubernetes.io/managed-by",
      .metadata.labels."app.kubernetes.io/instance",
      .spec.template.metadata.labels."helm.sh/chart",
      .spec.template.metadata.labels."app.kubernetes.io/managed-by",
      .spec.template.metadata.labels."app.kubernetes.io/instance"
    )
  ' -P - < "$1"
}

{
  echo "# Generated deploy manifest from charts/serverscom-rbs-csi"
  echo "# Secret and StorageClass objects are intentionally excluded"
  echo
  for file in \
    "$SRC/rbac.yaml" \
    "$SRC/iscsiadm-wrapper-configmap.yaml" \
    "$SRC/node-daemonset.yaml" \
    "$SRC/controller.yaml" \
    "$SRC/csi-driver.yaml"
  do
    clean "$file"
  done
} > "$OUTPUT"

echo "rendered $OUTPUT"
