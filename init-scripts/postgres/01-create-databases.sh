#!/bin/bash
# =============================================================================
#  Init Postgres mutualisé — crée 3 bases isolées + leurs utilisateurs dédiés.
#  Exécuté UNE SEULE FOIS, au tout premier démarrage (volume vide), par
#  l'entrypoint officiel postgres (/docker-entrypoint-initdb.d/).
#
#  Isolation : chaque brique a son utilisateur + sa base. Aucune ne peut lire
#  les tables d'une autre. C'est l'intérêt de la mutualisation maîtrisée :
#  1 conteneur, mais des frontières nettes.
# =============================================================================
set -euo pipefail

create_db_and_user() {
  local db="$1" user="$2" password="$3"
  echo ">> Création base '${db}' et utilisateur '${user}'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE USER ${user} WITH PASSWORD '${password}';
    CREATE DATABASE ${db} OWNER ${user};
    GRANT ALL PRIVILEGES ON DATABASE ${db} TO ${user};
EOSQL
}

# 1) TON Airflow
create_db_and_user "${AIRFLOW_DB}"    "${AIRFLOW_DB_USER}"    "${AIRFLOW_DB_PASSWORD}"
# 2) Airflow interne d'OpenMetadata
create_db_and_user "${OM_AIRFLOW_DB}" "${OM_AIRFLOW_DB_USER}" "${OM_AIRFLOW_DB_PASSWORD}"
# 3) Catalogue OpenMetadata
create_db_and_user "${OM_DB}"         "${OM_DB_USER}"         "${OM_DB_PASSWORD}"

echo ">> Init Postgres terminé : ${AIRFLOW_DB}, ${OM_AIRFLOW_DB}, ${OM_DB}"
