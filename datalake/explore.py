"""Exploration des CSV bruts (Jour 1, C18) — fonctions réutilisables.

Logique d'analyse PURE (renvoie des DataFrames/dicts, aucun effet de bord), de
sorte qu'elle soit appelée aussi bien par le notebook d'exploration que, plus
tard, par les DAGs d'ingestion/harmonisation.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")


def csv_paths(data_dir: Path | str = DATA_DIR) -> list[Path]:
    """Liste triée des fichiers CSV présents dans le dossier de données."""
    return sorted(Path(data_dir).glob("*.csv"))


def load(path: Path | str) -> pd.DataFrame:
    """Charge un CSV (colonnes telles quelles, pour révéler la casse réelle)."""
    return pd.read_csv(path)


def head(path: Path | str, n: int = 5) -> pd.DataFrame:
    """Les n premières lignes d'un fichier (aperçu du contenu brut)."""
    return load(path).head(n)


def describe(path: Path | str) -> pd.DataFrame:
    """Statistiques descriptives des colonnes numériques (min/max/moyenne…).

    Utile pour cerner les plages de valeurs normales par capteur (cf. C20).
    """
    return load(path).describe().round(2)


def _find_col(columns, name: str) -> str | None:
    """Retrouve une colonne par son nom, insensible à la casse."""
    for c in columns:
        if c.lower() == name.lower():
            return c
    return None


def profile(path: Path | str) -> dict:
    """Profil d'un fichier : volumétrie, NaN, présence elapsed_time, label."""
    path = Path(path)
    df = load(path)
    label_col = _find_col(df.columns, "label")
    return {
        "fichier": path.name,
        "lignes": len(df),
        "colonnes": df.shape[1],
        "taille_Ko": round(path.stat().st_size / 1024),
        "nb_NaN": int(df.isna().sum().sum()),
        "elapsed_time": _find_col(df.columns, "elapsed_time") is not None,
        "colonnes_brutes": list(df.columns),
        "label_valeurs": (
            sorted(df[label_col].dropna().unique().tolist())
            if label_col is not None else None
        ),
    }


def volumetry(paths: list[Path]) -> pd.DataFrame:
    """Tableau récapitulatif (une ligne par fichier)."""
    keep = ["fichier", "lignes", "colonnes", "taille_Ko", "nb_NaN", "elapsed_time"]
    rows = [{k: profile(p)[k] for k in keep} for p in paths]
    return pd.DataFrame(rows).set_index("fichier")


def casing_report(paths: list[Path]) -> pd.DataFrame:
    """Pour chaque colonne normalisée (minuscule), liste les variantes de casse
    rencontrées et le nombre de fichiers concernés — met en évidence l'hétérogénéité.
    """
    variants: dict[str, set[str]] = {}
    counts: dict[str, int] = {}
    for p in paths:
        for c in load(p).columns:
            key = c.lower()
            variants.setdefault(key, set()).add(c)
            counts[key] = counts.get(key, 0) + 1
    rows = [
        {
            "colonne (normalisée)": key,
            "variantes": ", ".join(sorted(variants[key])),
            "nb_variantes": len(variants[key]),
            "présente_dans_n_fichiers": counts[key],
        }
        for key in sorted(variants)
    ]
    return pd.DataFrame(rows).set_index("colonne (normalisée)")


def dtypes_table(paths: list[Path]) -> pd.DataFrame:
    """Types pandas inférés, par colonne normalisée et par fichier."""
    frames = {p.name: {c.lower(): str(t) for c, t in load(p).dtypes.items()} for p in paths}
    return pd.DataFrame(frames).rename_axis("colonne (normalisée)")


def label_distribution(paths: list[Path]) -> pd.DataFrame:
    """Distribution du `label` par fichier : valeurs, nombre et % d'anomalies.

    Permet de confronter le taux réel d'anomalies à celui annoncé dans la doc.
    """
    rows = []
    for p in paths:
        df = load(p)
        col = _find_col(df.columns, "label")
        n, k = len(df), int((df[col] == 1).sum())
        rows.append({
            "fichier": p.name,
            "valeurs": sorted(df[col].dropna().unique().tolist()),
            "lignes": n,
            "anomalies": k,
            "%_anomalies": round(100 * k / n, 2),
        })
    return pd.DataFrame(rows).set_index("fichier")


def line_id(filename: str) -> str:
    """Dérive l'identifiant de ligne (`lineA`…) depuis le nom de fichier.

    `LineA_Stable_10K.csv` -> `lineA`. Sert au chemin de partition
    `raw/production_lines/lineX/`.
    """
    m = re.search(r"Line([A-Z])", filename)
    return f"line{m.group(1)}" if m else Path(filename).stem


def coverage(paths: list[Path]) -> pd.DataFrame:
    """Couverture temporelle par fichier : période, cadence, continuité, partition.

    Fournit les éléments qui pilotent le partitionnement
    `raw/production_lines/lineX/year=YYYY/month=MM/` : identifiant de ligne,
    année, mois, et un contrôle de régularité (1 relevé/minute, sans trou).
    """
    rows = []
    for p in paths:
        df = load(p)
        ts = pd.to_datetime(df[_find_col(df.columns, "timestamp")])
        step = ts.diff().dropna().mode().iloc[0]
        n = len(ts)
        attendu = int((ts.max() - ts.min()) / step) + 1
        continu = bool(ts.is_monotonic_increasing and ts.nunique() == n and attendu == n)
        rows.append({
            "fichier": p.name,
            "ligne": line_id(p.name),
            "debut": ts.min(),
            "fin": ts.max(),
            "n": n,
            "cadence": str(step),
            "continu": continu,
            "year": int(ts.min().year),
            "month": f"{ts.min().month:02d}",
            "mois_unique": bool(ts.dt.to_period("M").nunique() == 1),
        })
    return pd.DataFrame(rows).set_index("fichier")
