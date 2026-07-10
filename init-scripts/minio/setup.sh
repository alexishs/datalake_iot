#!/bin/sh
# =============================================================================
#  Initialisation MinIO — exécuté par le job one-shot `minio-init`.
#  Crée : les 4 buckets (un par couche) + les comptes de service et leurs
#  policies IAM différenciées par bucket (C19 « policies d'accès initiales »).
#
#  Idempotent : relançable sans erreur. Les commandes susceptibles d'échouer
#  « parce que ça existe déjà » sont neutralisées par `|| true`.
#
#  Rappel modèle : une policy MinIO (IAM, JSON) liste des actions S3 sur des
#  ARN de buckets, puis est ATTACHÉE à un utilisateur. Les policies intégrées
#  (readonly/readwrite) portent sur TOUS les buckets -> on utilise des policies
#  personnalisées pour restreindre « selon bucket ».
# =============================================================================
set -eu

POLICIES="$(dirname "$0")/policies"

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# --- Buckets : un par couche ------------------------------------------------
for bucket in raw staging curated archive; do
  mc mb --ignore-existing "local/$bucket"
done

# --- Chiffrement au repos SSE-S3 sur les 4 buckets (C21) --------------------
# Auto-chiffrement : tout objet écrit ENSUITE est chiffré côté serveur via le
# KMS intégré (clé MINIO_KMS_SECRET_KEY). Non rétroactif : les objets déjà
# présents restent en clair (cf. rapport). Requiert le KMS actif sur le serveur.
for bucket in raw staging curated archive; do
  mc encrypt set sse-s3 "local/$bucket"
done
echo "SSE-S3 : auto-chiffrement activé sur raw/staging/curated/archive."

# --- Policies IAM personnalisées (droits par bucket) ------------------------
# `create` échoue si la policy existe déjà -> ignoré pour l'idempotence.
mc admin policy create local data-analyst  "$POLICIES/data-analyst.json"  2>/dev/null || true
mc admin policy create local data-engineer "$POLICIES/data-engineer.json" 2>/dev/null || true

# --- Comptes de service + attachement ---------------------------------------
# data-analyst : LECTURE SEULE sur curated/
mc admin user add local "$MINIO_ANALYST_USER" "$MINIO_ANALYST_PASSWORD" 2>/dev/null || true
mc admin policy attach local data-analyst --user "$MINIO_ANALYST_USER" 2>/dev/null || true

# data-engineer : LECTURE/ÉCRITURE sur raw/ + staging/ + curated/
mc admin user add local "$MINIO_ENGINEER_USER" "$MINIO_ENGINEER_PASSWORD" 2>/dev/null || true
mc admin policy attach local data-engineer --user "$MINIO_ENGINEER_USER" 2>/dev/null || true

# admin : TOUS DROITS (policy intégrée consoleAdmin) — root MinIO l'est déjà
mc admin user add local "$MINIO_ADMIN_USER" "$MINIO_ADMIN_PASSWORD" 2>/dev/null || true
mc admin policy attach local consoleAdmin --user "$MINIO_ADMIN_USER" 2>/dev/null || true

# --- Cycle de vie (ILM) : suppression des objets archivés après ~2 ans (730 j) ---
# Fondé sur l'ÂGE DES OBJETS (date d'upload). Nos objets datant de 2026, la règle
# est configurée mais ne se déclenchera pas avant ~2 ans (documenté, non démontrable).
# rm --all puis add -> idempotent (relançable sans empiler les règles).
mc ilm rule rm --all --force local/archive 2>/dev/null || true
mc ilm rule add local/archive --expire-days 730
echo "ILM : expiration 730 j configurée sur archive/."

echo "MinIO prêt : buckets raw/staging/curated/archive + comptes data-analyst (RO curated),"
echo "             data-engineer (RW raw/staging/curated), admin (tous droits)."
