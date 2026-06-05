"""Mécanique partagée du pipeline : exécuteur d'actions + plomberie du fil-de-l'eau.

Mutualise la mécanique (boucle, capture d'erreurs, rapport, code de sortie) entre les CLI
des étapes, ainsi que le **fil-de-l'eau** des étapes de transformation : `filigrane`
(plus ancien jour manquant en aval), `pas` (un incrément) et `drainer` (boucle CLI).
Aucune logique métier ici : `runner` ignore tout du contenu des couches.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any

from botocore.client import BaseClient


@dataclass
class Result:
    label: str   # ce qui est traité (p. ex. nom de fichier)
    statut: str  # texte court : "déposé", "inchangé (MD5)", "ré-importé", "ÉCHEC MD5"…
    ok: bool     # succès


def run(action: Callable[[Any], Result], items: Iterable[Any], titre: str) -> int:
    """Applique `action` à chaque item. Renvoie 0 si tout OK, 1 si au moins un échec."""
    print(titre)
    ok_count = ko_count = 0
    for item in items:
        try:
            res = action(item)
        except Exception as exc:  # garde-fou, réseau, S3… : un échec n'arrête pas les autres
            res = Result(str(item), f"ERREUR : {exc}", False)
        print(f"  {'✓' if res.ok else '✗'} {res.label} — {res.statut}")
        if res.ok:
            ok_count += 1
        else:
            ko_count += 1
    print(f"→ {ok_count} OK, {ko_count} échec(s).")
    return 1 if ko_count else 0


def checked(result: Result) -> str:
    """Renvoie un libellé « label — statut » si succès, lève `RuntimeError` sinon.

    Adaptateur pour Airflow : une tâche échoue (rouge) si l'exception remonte.
    La logique métier (qui produit le `Result`) reste, elle, dans le package.
    """
    libelle = f"{result.label} — {result.statut}"
    if not result.ok:
        raise RuntimeError(libelle)
    return libelle


# --- Fil-de-l'eau : plomberie partagée par harmonization et consolidation -------------

def filigrane(amont: dict[str, set[date]], aval: dict[str, set[date]]) -> date | None:
    """Plus ancien `(clé, jour)` présent en amont mais absent de l'aval, sinon `None`.

    Cœur du fil-de-l'eau **auto-réparant** : un trou créé en aval (cascade) redevient
    automatiquement le prochain jour à traiter. Fonction pure (aucune I/O).
    """
    manquants = {d for cle, jours in amont.items() for d in jours if d not in aval.get(cle, set())}
    return min(manquants) if manquants else None


def pas(
    client: BaseClient,
    jour_a_traiter: Callable[[BaseClient], date | None],
    traiter_jour: Callable[[BaseClient, date], list[Result]],
    label_aval: str,
) -> Result:
    """Un pas de pipeline : traite la **plus ancienne** journée en attente (filigrane).

    Renvoie `Result(label_aval, "à jour", True)` s'il n'y a rien à faire. Sinon agrège
    le résultat de `traiter_jour` en un `Result` (succès si toutes les lignes du jour ont réussi).
    """
    jour = jour_a_traiter(client)
    if jour is None:
        return Result(label_aval, "à jour", True)
    res = traiter_jour(client, jour)
    statut = f"{len(res)} ligne(s) de production traitée(s)"
    return Result(str(jour), statut, all(r.ok for r in res))


def drainer(client: BaseClient, faire_un_pas: Callable[[BaseClient], Result], titre: str) -> int:
    """Draine en boucle (un pas à la fois) jusqu'à « à jour ». Renvoie 0 si OK, 1 sinon.

    Usage CLI manuel (`python -m datalake.harmonization`) : rejoue le pas jusqu'à épuisement.
    """
    print(titre)
    while True:
        r = faire_un_pas(client)
        print(f"  {'✓' if r.ok else '✗'} {r.label} — {r.statut}")
        if not r.ok:
            return 1
        if r.statut == "à jour":
            return 0
