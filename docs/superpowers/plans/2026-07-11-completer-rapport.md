# Compléter le rapport (5-6 pages) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transformer `rapport/rapport.md` en un rapport professionnel **autonome, thématique, de 5 à 6 pages**, décrivant uniquement ce qui a été réalisé, avec figures compactes et liens vers la documentation.

**Architecture:** Réécriture **en place** d'un seul fichier (`rapport/rapport.md`), section par section, en consolidant la matière du **journal** (`rapport/journal.md`) et des docs existantes (`docs/architecture.md`, `docs/gouvernance-*.md`, `init-scripts/openmetadata/README.md`). Aucun code, aucun test unitaire : la validation se fait par **relecture de couverture** + contrôles (longueur, liens, vouvoiement, absence d'éléments non réalisés). Spec : [docs/superpowers/specs/2026-07-11-completer-rapport-design.md](../specs/2026-07-11-completer-rapport-design.md).

**Tech Stack:** Markdown (français), figures compactes (tableaux + diagrammes texte). Export PDF **hors périmètre**.

## Global Constraints

- **AUCUN commit** dans les étapes : l'utilisateur committe après relecture. Les tâches s'arrêtent à « vérification verte ». Messages de commit proposés en français, **sans mention d'agent IA**.
- **Uniquement ce qui a été fait.** **Omission totale** des éléments non réalisés : ni logs d'audit, ni ségrégation par ligne, ni section « écarts / perspectives », ni « Airflow d'OpenMetadata supprimable ». L'auto-évaluation C21 est **nettoyée** de « (hors audit) » et de la phrase sur l'audit.
- **Autonome + liens** : le rapport se suffit à lui-même mais **renvoie** aux autres docs (doublons assumés).
- **Langage clair et pédagogique** pour un professionnel IT (expliquer le *pourquoi*).
- **Français, vouvoiement** ; Markdown **sans hard-wrap** (un paragraphe/puce = une ligne).
- **Figures compactes uniquement** : aucune ne doit occuper une demi-page. Les **captures d'écran** OpenMetadata sont **référencées par lien** (jamais insérées pleine largeur).
- **Longueur cible 5 à 6 pages** (≈ 2500-3200 mots), **sans dépasser 6**.
- **Exécution recommandée : inline** (un seul document → voix et cohérence homogènes ; le découpage en sous-agents fragmenterait le style).

## Sources de matière (à consulter pendant la rédaction)

- `rapport/journal.md` — entrées datées (activités + notions par jour) : la matière brute à réorganiser thématiquement.
- `docs/architecture.md` — architecture en couches, partitionnement, choix, filigrane (§3.2), justifications.
- `docs/gouvernance-cycle-de-vie.md` — cycle de vie (archivage DAG + ILM, réintégration).
- `docs/gouvernance-acces-securite.md` — matrice des droits, SSE-S3, responsabilités.
- `init-scripts/openmetadata/README.md` — procédure catalogue config-as-code.
- `rapport/rapport.md` (actuel) — §1 contexte, §2 auto-évaluation, §3 annexes à reprendre/étoffer.

## Figures (contenu exact à réutiliser)

**F1 — Schéma en couches (§2)**, bloc compact :

```
data/ (CSV source)
   │  ingestion (byte-identique + MD5)
   ▼
raw/       production_lines/lineX/year=/month=/           (brut, immuable)
   │  harmonisation (Parquet, schéma unifié)
   ▼
staging/   production_lines/lineX/year=/month=/day=/      (nettoyé)
   │  consolidation (table unifiée)
   ▼
curated/   production_lines/line=lineX/year=/month=/day=/ (prêt à l'analyse)
   │  archivage (DAG) ─────────────► archive/  (rétention, expiration ILM)
```

**F2 — Hétérogénéité des schémas `raw` (§3 C18)**, table Markdown :

| Concept | lineA | lineB | lineC | lineD | lineE |
|---|---|---|---|---|---|
| `timestamp` | `timestamp` | `timestamp` | `timestamp` | `timestamp` | `timestamp` |
| `temperature` | `Temperature` | `temperature` | `Temperature` | `temperature` | `Temperature` |
| `pressure` | `pressure` | `pressure` | `pressure` | `Pressure` | `pressure` |
| `elapsed_time` | `elapsed_time` | `Elapsed_time` | ❌ absent | ❌ absent | ❌ absent |
| `label` | `label` | `label` | `label` | `label` | `label` |

**F3 — Lignage (§3 C20)**, bloc compact :

```
raw.lineX ─(harmonisation_staging)→ staging.lineX ─(consolidation_curated)→ curated.line=lineX
raw.lineE ─(archivage)→ archive.lineE
```

**F4 — Matrice des droits (§3 C21)**, table Markdown :

| Rôle (compte) | raw | staging | curated | archive |
|---|---|---|---|---|
| `data-analyst` | — | — | lecture | — |
| `data-engineer` | lecture/écriture | lecture/écriture | lecture/écriture | lecture/écriture |
| `datalake-admin` | tous droits | tous droits | tous droits | tous droits |

---

## Task 1 : Introduction, contexte & architecture (§1–§2)

**Files:** Modifier `rapport/rapport.md` (remplace le titre + §1 actuel ; ajoute §2).

- [ ] **Step 1 — Titre & §1 Introduction & contexte.** Rédiger : mission (équipementier automobile, ESN IndustrIA), données de **5 lignes de production** instrumentées (température, pression, temps de fonctionnement) stockées « en vrac », finalité **maintenance prédictive**, **stack imposée** (MinIO, Airflow, OpenMetadata, Python/boto3, Docker Compose, Git). Conserver la note « démarche : socle technique d'abord » et le renvoi au [journal](journal.md) pour la chronologie. Lien : [README](../README.md).
- [ ] **Step 2 — §2 Architecture du data lake.** Rédiger : les **4 couches** (`raw`/`staging`/`curated`/`archive`) et leur rôle ; insérer **F1** ; expliquer le **partitionnement à deux granularités** (`raw` au **mois** — fichiers déposés tels quels ; `staging`/`curated` au **jour** — traitement au fil de l'eau et requêtes journalières) ; justifier **MinIO** (objet compatible S3) et **CSV→Parquet** (source intacte en `raw` ; typage/compression/colonne en aval). Renvoi : [docs/architecture.md](../docs/architecture.md).
- [ ] **Step 3 — Vérifier** : `grep -nE "^## " rapport/rapport.md` montre §1, §2 ; `grep -rnE "\b(tu|ton|ta|tes|toi)\b" rapport/rapport.md || echo OK` → OK ; relire que le *pourquoi* des choix est explicité (pédagogique).

---

## Task 2 : Réalisations par compétence (§3)

**Files:** Modifier `rapport/rapport.md` (ajoute §3, le cœur du rapport).

**Interfaces :** consomme F2 (C18), F3 (C20), F4 (C21).

- [ ] **Step 1 — §3.1 C18 — Analyse & architecture.** Rédiger : exploration des 5 lignes (volumétrie — LineA 10 000 enreg., `label` 0/1 et **déséquilibre de classes**) menée **avant** les décisions d'architecture ; l'**hétérogénéité des schémas** (insérer **F2**) comme difficulté centrale ; décisions d'architecture qui en découlent (schéma cible unifié, partitionnement). Liens : [docs/architecture.md](../docs/architecture.md), [notebooks/exploration_jour1.ipynb](../notebooks/exploration_jour1.ipynb).
- [ ] **Step 2 — §3.2 C19 — Intégration.** Rédiger : stack Docker **reproductible** (relançable par un tiers) ; **ingestion `raw`** byte-identique + **contrôle d'intégrité MD5 (ETag)** + **idempotence** ; **3 DAGs** en fil-de-l'eau (`ingestion_raw` → `harmonisation_staging` → `consolidation_curated`) en **coquilles fines** (logique dans le package `datalake/`) ; harmonisation des schémas vers le schéma cible. Liens : [README](../README.md), [dags/](../dags/).
- [ ] **Step 3 — §3.3 C20 — Catalogue & cycle de vie.** Rédiger : **catalogue OpenMetadata** en **config-as-code** (service S3 `datalake_minio`, 16 conteneurs, **5 fiches `raw` enrichies** — description, propriétaire, source Zenodo, fréquence, colonnes/unités) ; **lignage** de bout en bout (insérer **F3**) ; **cycle de vie** (archivage `raw→archive` par **DAG** car l'ILM objet ne copie pas localement ; **expiration** par règle **ILM** 730 j). Renvoyer aux **captures** (sans les insérer en grand) : [docs/captures-openmetadata/](../docs/captures-openmetadata/). Liens : [init-scripts/openmetadata/README.md](../init-scripts/openmetadata/README.md), [docs/gouvernance-cycle-de-vie.md](../docs/gouvernance-cycle-de-vie.md).
- [ ] **Step 4 — §3.4 C21 — Sécurité & gouvernance.** Rédiger : **3 comptes** de service et **policies IAM par bucket** (moindre privilège) ; insérer **F4** ; **chiffrement SSE-S3** au repos (KMS intégré, transparent côté client) ; **politique de gouvernance** (qui accède à quoi, responsabilités par rôle). Lien : [docs/gouvernance-acces-securite.md](../docs/gouvernance-acces-securite.md). **Ne pas mentionner** l'audit ni la ségrégation par ligne.
- [ ] **Step 5 — Vérifier** : les 4 sous-sections et les figures F2/F3/F4 sont présentes ; `grep -n "audit" rapport/rapport.md || echo "OK pas d'audit"` → OK ; vouvoiement OK.

---

## Task 3 : Choix techniques & difficultés, auto-évaluation, conclusion (§4–§6)

**Files:** Modifier `rapport/rapport.md`.

- [ ] **Step 1 — §4 Choix techniques & difficultés.** Rédiger le *pourquoi* des décisions **prises** : **config-as-code** du catalogue, **2 Airflow séparés** (métier vs OpenMetadata), exploration SQL **DuckDB** (analyste, lecture directe de Parquet sur S3). Difficultés rencontrées : hétérogénéité des schémas, **sensibilité de versions d'OpenMetadata**, **limites de l'ILM objet** (pas de copie locale bucket-à-bucket → archivage par DAG). Notions abordées (synthèse : IAM/policies, idempotence, intégrité MD5, `null` natif Polars, gouvernance des métadonnées, lignage, chiffrement au repos).
- [ ] **Step 2 — §4 (paragraphe dédié) Filigrane auto-réparant + réintégration.** Rédiger **5-6 phrases** en trois temps (cf. spec) : (a) **mécanisme** — pipeline piloté par **l'état des données** (« plus ancien jour manquant en `staging` »), pas par la date d'exécution ; une journée par run ; (b) **réintégration transparente** — recopier un mois de `archive/` vers `raw/` déclenche la **purge en cascade** des dérivés → le **filigrane recule** → `staging` puis `curated` **se recréent sans action manuelle** (c'est **filigrane + purge en cascade** ensemble) ; (c) **trade-off honnête** — ce balayage « un jour par run » convient à la **simulation d'un flux** et au **backfill**, mais **pas à un vrai flux** : en production on indexerait sur la **date d'exécution / l'intervalle de données** (traitement *en avant*) avec un **backfill explicite** ; notre logique n'a de sens que **parce qu'on réimporte des jours passés**. Cadrer comme **choix de conception assumé** (maturité), pas comme défaut.
- [ ] **Step 3 — §5 Auto-évaluation par compétence.** Reprendre l'actuelle liste C18–C21, en **nettoyant C21** : retirer « (hors audit) » et la phrase « Les logs d'audit ne sont pas activés… ». C21 devient : *acquis* — 3 comptes différenciés par bucket, chiffrement SSE-S3 au repos, matrice des droits + politique de gouvernance. Retirer aussi de la puce « Recul sur les choix » toute mention d'élément non fait.
- [ ] **Step 4 — §6 Conclusion.** Rédiger **une brève conclusion** (4-6 phrases) : bilan de ce qui a été **livré** — data lake **opérationnel, reproductible, catalogué et sécurisé**, fondation prête pour l'exploitation. **Sans** liste de travaux futurs ni éléments non réalisés.
- [ ] **Step 5 — Vérifier** : §4, §5, §6 présents ; le paragraphe filigrane couvre bien les 3 temps ; `grep -niE "audit|ségrégation par ligne|perspectives" rapport/rapport.md || echo OK` → OK ; vouvoiement OK.

---

## Task 4 : Annexes & vérification globale (§7 + contrôle final)

**Files:** Modifier `rapport/rapport.md`.

- [ ] **Step 1 — §7 Annexes.** Conserver/adapter la liste actuelle : index de la documentation (README, architecture, gouvernance ×2, init-scripts/openmetadata/README, captures, journal) + ressources (notebook, dépôt Git). Vérifier que les liens pointent bien depuis `rapport/` (préfixe `../` sauf `journal.md`).
- [ ] **Step 2 — Contrôle des liens.** Lancer :

```bash
cd /home/alexis/projets/datalake_iot && python3 - <<'PY'
import re,os
f="rapport/rapport.md"; base=os.path.dirname(f)
txt=open(f,encoding="utf-8").read()
bad=[l for l in re.findall(r'\]\(([^)#]+)(?:#[^)]*)?\)', txt)
     if not l.startswith(('http','mailto')) and not os.path.exists(os.path.normpath(os.path.join(base,l.split('#')[0])))]
print("liens morts :", bad or "aucun")
PY
```
Expected : `aucun`.

- [ ] **Step 3 — Contrôle longueur (5-6 pages).**

```bash
wc -w rapport/rapport.md
```
Expected : **≈ 2500-3200 mots** (5-6 pages). Si < 2500 : étoffer le *pourquoi* des choix (pédagogie) ; si > 3200 : resserrer. **Ne pas dépasser ~3200 mots.**

- [ ] **Step 4 — Contrôles finaux.**

```bash
grep -rnE "\b(tu|ton|ta|tes|toi)\b" rapport/rapport.md || echo "OK vouvoiement"
grep -niE "\baudit\b|ségrégation par ligne|évolutions possibles|perspectives|supprimable" rapport/rapport.md || echo "OK aucun élément non réalisé"
```
Expected : `OK vouvoiement` et `OK aucun élément non réalisé`.

- [ ] **Step 5 — Relecture d'ensemble** : le rapport se lit comme **un tout cohérent et autonome** (voix homogène, transitions), figures **compactes** (aucune capture insérée pleine largeur), langage **clair et pédagogique**. Arrêt (pas de commit).

> Message de commit proposé (pour l'utilisateur) : `docs(rapport) : rapport professionnel complété (5-6 pages, thématique, autonome)`.

---

## Auto-revue (fin de plan)

- **Couverture spec :** §1-2 (T1), §3 C18-C21 + F2/F3/F4 (T2), §4 filigrane + choix/difficultés + §5 auto-éval nettoyée + §6 conclusion (T3), §7 annexes + vérifs longueur/liens/vouvoiement/omission (T4). ✅
- **Omission des non-faits :** nettoyage C21 (T3.3), contrôle `grep audit|…` (T3.5, T4.4). ✅
- **Figures compactes :** F1-F4 natives ; captures **référencées** (T2.3). ✅
- **Longueur 5-6 pages** contrôlée (T4.3) ; **liens** (T4.2) ; **vouvoiement** (T1.3, T2.5, T3.5, T4.4).
- **AUCUN commit** dans les étapes ; message proposé sans mention IA.
- **Cohérence des noms** : couches `raw/staging/curated/archive`, comptes `data-analyst`/`data-engineer`/`datalake-admin`, figures F1-F4 — constants entre tâches.
