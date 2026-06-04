"""Exécuteur générique : applique une action à des items, rapporte, renvoie un code de sortie.

Mutualise la mécanique (boucle, capture d'erreurs, rapport) entre les CLI des étapes
du pipeline. Chaque action renvoie un `Result` ; le `runner` n'a aucune logique métier.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any


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
