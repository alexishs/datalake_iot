#!/usr/bin/env bash
# =============================================================================
#  C20 — Ingestion du data lake dans OpenMetadata (config-as-code, reproductible).
#
#  Étapes :
#    1. dépose les manifests à la RACINE de chaque bucket (raw/staging/curated/
#       archive) sous le nom attendu `openmetadata_storage_manifest.json` ;
#    2. substitue les secrets (${...}) de la spec YAML depuis .env ;
#    3. lance `metadata ingest` dans le conteneur d'ingestion OpenMetadata.
#
#  Prérequis : stack `docker compose up` démarrée ; .env renseigné (dont
#  OPENMETADATA_JWT_TOKEN, copié depuis l'UI OpenMetadata > Settings > Bots >
#  ingestion-bot). Aucun secret n'est affiché ni écrit dans le dépôt.
#
#  Usage :  ./init-scripts/openmetadata/run-ingestion.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"
MINIO_CONTAINER="dl-minio"
OM_INGESTION_CONTAINER="dl-om-ingestion"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERREUR : .env introuvable ($ENV_FILE). Copiez .env.example en .env." >&2
  exit 1
fi

# Charge uniquement les variables nécessaires (sans les afficher).
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

for var in MINIO_ROOT_USER MINIO_ROOT_PASSWORD OPENMETADATA_JWT_TOKEN; do
  if [[ -z "${!var:-}" || "${!var}" == change-me* ]]; then
    echo "ERREUR : variable $var absente ou non renseignée dans .env." >&2
    exit 1
  fi
done

echo "1/3 — Dépôt des manifests à la racine des buckets…"
# Le connecteur S3 lit un manifest LOCAL nommé exactement `openmetadata.json`
# (constante OPENMETADATA_TEMPLATE_FILE_NAME) à la racine de chaque bucket.
for bucket in raw staging curated archive; do
  manifest="$SCRIPT_DIR/manifests/${bucket}.openmetadata.json"
  [[ -f "$manifest" ]] || { echo "  manifest manquant : $manifest" >&2; exit 1; }
  docker exec -i "$MINIO_CONTAINER" sh -c '
    mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1
    mc pipe "local/'"$bucket"'/openmetadata.json"
  ' < "$manifest"
  echo "  ✓ $bucket/openmetadata.json"
done

echo "2/3 — Substitution des secrets dans la spec YAML…"
RESOLVED="$(mktemp)"
trap 'rm -f "$RESOLVED"' EXIT
export MINIO_ROOT_USER MINIO_ROOT_PASSWORD OPENMETADATA_JWT_TOKEN
envsubst '${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} ${OPENMETADATA_JWT_TOKEN}' \
  < "$SCRIPT_DIR/s3-storage-ingestion.yaml" > "$RESOLVED"

echo "3/3 — Lancement de l'ingestion OpenMetadata…"
# 644 : lisible par l'utilisateur `airflow` du conteneur (docker cp conserve le
# mode du fichier hôte, sinon 600/uid-hôte → « Permission denied » à la lecture).
chmod 644 "$RESOLVED"
docker cp "$RESOLVED" "$OM_INGESTION_CONTAINER:/tmp/s3-storage-ingestion.yaml"
docker exec "$OM_INGESTION_CONTAINER" metadata ingest -c /tmp/s3-storage-ingestion.yaml
# Suppression en root (fichier possédé par l'uid hôte dans /tmp à sticky bit).
docker exec -u root "$OM_INGESTION_CONTAINER" rm -f /tmp/s3-storage-ingestion.yaml

echo "Terminé. Vérifiez dans l'UI OpenMetadata (Explore > Containers) le service 'datalake_minio'."
