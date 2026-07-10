#!/usr/bin/env bash
# =============================================================================
#  C20 — Enrichissement des fiches OpenMetadata (propriétaire, descriptions,
#  colonnes). Exécute enrich.py DANS le conteneur d'ingestion (SDK `metadata`).
#
#  Prérequis : ingestion des conteneurs déjà faite (run-ingestion.sh) ; .env
#  renseigné (OPENMETADATA_JWT_TOKEN). Aucun secret affiché.
#
#  Usage :  ./init-scripts/openmetadata/run-enrichment.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"
OM_INGESTION_CONTAINER="dl-om-ingestion"

[[ -f "$ENV_FILE" ]] || { echo "ERREUR : .env introuvable ($ENV_FILE)." >&2; exit 1; }
set -a; source "$ENV_FILE"; set +a
if [[ -z "${OPENMETADATA_JWT_TOKEN:-}" || "${OPENMETADATA_JWT_TOKEN}" == change-me* ]]; then
  echo "ERREUR : OPENMETADATA_JWT_TOKEN absent ou non renseigné dans .env." >&2
  exit 1
fi

docker cp "$SCRIPT_DIR/enrich.py" "$OM_INGESTION_CONTAINER:/tmp/enrich.py"
docker exec \
  -e OPENMETADATA_HOST_PORT="http://openmetadata-server:8585/api" \
  -e OPENMETADATA_JWT_TOKEN="$OPENMETADATA_JWT_TOKEN" \
  "$OM_INGESTION_CONTAINER" python /tmp/enrich.py
docker exec -u root "$OM_INGESTION_CONTAINER" rm -f /tmp/enrich.py
