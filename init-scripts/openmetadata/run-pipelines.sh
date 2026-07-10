#!/usr/bin/env bash
# =============================================================================
#  C20 — Ingestion des pipelines Airflow + lignage dans OpenMetadata.
#
#  Substitue les secrets de airflow-pipeline-ingestion.yaml depuis .env, puis
#  lance `metadata ingest` dans le conteneur d'ingestion. Catalogue les 4 DAGs
#  comme Pipelines et trace le lignage entre conteneurs (inlets/outlets).
#
#  Prérequis : ingestion des conteneurs déjà faite (run-ingestion.sh) — les
#  conteneurs source/cible doivent exister pour que les arêtes soient tracées.
#
#  Usage :  ./init-scripts/openmetadata/run-pipelines.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"
OM_INGESTION_CONTAINER="dl-om-ingestion"

[[ -f "$ENV_FILE" ]] || { echo "ERREUR : .env introuvable ($ENV_FILE)." >&2; exit 1; }
set -a; source "$ENV_FILE"; set +a
for var in AIRFLOW_DB AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD OPENMETADATA_JWT_TOKEN; do
  if [[ -z "${!var:-}" || "${!var}" == change-me* ]]; then
    echo "ERREUR : variable $var absente ou non renseignée dans .env." >&2
    exit 1
  fi
done

RESOLVED="$(mktemp)"
trap 'rm -f "$RESOLVED"' EXIT
export AIRFLOW_DB AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD OPENMETADATA_JWT_TOKEN
envsubst '${AIRFLOW_DB} ${AIRFLOW_DB_USER} ${AIRFLOW_DB_PASSWORD} ${OPENMETADATA_JWT_TOKEN}' \
  < "$SCRIPT_DIR/airflow-pipeline-ingestion.yaml" > "$RESOLVED"

chmod 644 "$RESOLVED"
docker cp "$RESOLVED" "$OM_INGESTION_CONTAINER:/tmp/airflow-pipeline-ingestion.yaml"
docker exec "$OM_INGESTION_CONTAINER" metadata ingest -c /tmp/airflow-pipeline-ingestion.yaml
docker exec -u root "$OM_INGESTION_CONTAINER" rm -f /tmp/airflow-pipeline-ingestion.yaml

echo "Terminé. Vérifiez dans l'UI : service 'datalake_airflow' (Pipelines) + le lignage des conteneurs."
