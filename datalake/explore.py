"""Exploration des CSV bruts (Jour 1, C18) — fonctions réutilisables.

Logique d'analyse PURE (renvoie des DataFrames/dicts, aucun effet de bord), de
sorte qu'elle soit appelée aussi bien par le notebook d'exploration que, plus
tard, par les DAGs d'ingestion/harmonisation. Basé sur **Polars** (Arrow-natif).
"""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

DATA_DIR = Path("data")
TS_FORMAT = "%Y-%m-%d %H:%M:%S"  # format du timestamp source (non ISO-T)


def csv_paths(data_dir: Path | str = DATA_DIR) -> list[Path]:
    """Liste triée des fichiers CSV présents dans le dossier de données."""
    return sorted(Path(data_dir).glob("*.csv"))


def load(path: Path | str) -> pl.DataFrame:
    """Charge un CSV (colonnes telles quelles, pour révéler la casse réelle)."""
    return pl.read_csv(path)


def head(path: Path | str, n: int = 5) -> pl.DataFrame:
    """Les n premières lignes d'un fichier (aperçu du contenu brut)."""
    return load(path).head(n)


def describe(path: Path | str) -> pl.DataFrame:
    """Statistiques descriptives (min/max/moyenne…) — utile pour les plages (C20)."""
    return load(path).describe()


def _find_col(columns, name: str) -> str | None:
    """Retrouve une colonne par son nom, insensible à la casse."""
    for c in columns:
        if c.lower() == name.lower():
            return c
    return None


def profile(path: Path | str) -> dict:
    """Profil d'un fichier : volumétrie, valeurs manquantes, elapsed_time, label."""
    path = Path(path)
    df = load(path)
    label_col = _find_col(df.columns, "label")
    return {
        "fichier": path.name,
        "lignes": df.height,
        "colonnes": df.width,
        "taille_Ko": round(path.stat().st_size / 1024),
        "nb_manquants": int(sum(df.null_count().row(0))),
        "elapsed_time": _find_col(df.columns, "elapsed_time") is not None,
        "colonnes_brutes": df.columns,
        "label_valeurs": (
            sorted(df[label_col].drop_nulls().unique().to_list())
            if label_col is not None else None
        ),
    }


def volumetry(paths: list[Path]) -> pl.DataFrame:
    """Tableau récapitulatif (une ligne par fichier)."""
    keep = ["fichier", "lignes", "colonnes", "taille_Ko", "nb_manquants", "elapsed_time"]
    return pl.DataFrame([{k: profile(p)[k] for k in keep} for p in paths])


def casing_report(paths: list[Path]) -> pl.DataFrame:
    """Pour chaque colonne normalisée : variantes de casse et nb de fichiers.

    En conservant la casse d'origine, expose l'hétérogénéité `Temperature` vs
    `temperature`, etc.
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
    return pl.DataFrame(rows)


def dtypes_table(paths: list[Path]) -> pl.DataFrame:
    """Types inférés, par colonne normalisée et par fichier."""
    per_file = {
        p.name: {c.lower(): str(t) for c, t in zip(load(p).columns, load(p).dtypes)}
        for p in paths
    }
    all_cols = sorted({c for d in per_file.values() for c in d})
    rows = [
        {"colonne (normalisée)": c, **{name: d.get(c) for name, d in per_file.items()}}
        for c in all_cols
    ]
    return pl.DataFrame(rows)


def label_distribution(paths: list[Path]) -> pl.DataFrame:
    """Distribution du `label` par fichier : valeurs, nombre et % d'anomalies."""
    rows = []
    for p in paths:
        df = load(p)
        col = _find_col(df.columns, "label")
        n = df.height
        k = int(df.filter(pl.col(col) == 1).height)
        rows.append({
            "fichier": p.name,
            "valeurs": sorted(df[col].drop_nulls().unique().to_list()),
            "lignes": n,
            "anomalies": k,
            "%_anomalies": round(100 * k / n, 2),
        })
    return pl.DataFrame(rows)


def line_id(filename: str) -> str:
    """Dérive l'identifiant de ligne (`lineA`…) depuis le nom de fichier.

    `LineA_Stable_10K.csv` -> `lineA`. Sert au chemin de partition.
    """
    m = re.search(r"Line([A-Z])", filename)
    return f"line{m.group(1)}" if m else Path(filename).stem


def coverage(paths: list[Path]) -> pl.DataFrame:
    """Couverture temporelle par fichier : période, cadence, continuité, partition.

    Fournit ce qui pilote le partitionnement (ligne, année, mois) et un contrôle
    de régularité (1 relevé/minute, sans trou).
    """
    rows = []
    for p in paths:
        df = load(p)
        ts = df.get_column(_find_col(df.columns, "timestamp")).str.to_datetime(TS_FORMAT).sort()
        n = ts.len()
        step = ts.diff().drop_nulls().mode().to_list()[0]   # timedelta le plus fréquent
        debut, fin = ts.min(), ts.max()
        attendu = int((fin - debut) / step) + 1
        continu = bool(ts.n_unique() == n and attendu == n)
        mois_unique = bool(ts.dt.year().n_unique() == 1 and ts.dt.month().n_unique() == 1)
        rows.append({
            "fichier": p.name,
            "ligne": line_id(p.name),
            "debut": debut,
            "fin": fin,
            "n": n,
            "cadence": str(step),
            "continu": continu,
            "year": debut.year,
            "month": f"{debut.month:02d}",
            "mois_unique": mois_unique,
        })
    return pl.DataFrame(rows)
