from __future__ import annotations

from datetime import date

import pytest

from datalake.runner import Result, checked, drainer, filigrane, pas, run


def test_run_all_ok_returns_0(capsys: pytest.CaptureFixture[str]) -> None:
    items = ["a", "b"]
    code = run(lambda x: Result(x, "ok", True), items, "Titre")
    assert code == 0
    out = capsys.readouterr().out
    assert "Titre" in out and "2 OK, 0 échec(s)." in out


def test_run_with_failure_returns_1() -> None:
    code = run(lambda x: Result(x, "ko", x == "a"), ["a", "b"], "Titre")
    assert code == 1  # 'b' échoue


def test_run_captures_exceptions_and_continues() -> None:
    seen = []

    def action(x: str) -> Result:
        seen.append(x)
        if x == "boom":
            raise ValueError("explosion")
        return Result(x, "ok", True)

    code = run(action, ["x", "boom", "y"], "Titre")
    assert code == 1
    assert seen == ["x", "boom", "y"]  # tous les items traités malgré l'exception


def test_checked_ok_renvoie_le_libelle() -> None:
    assert checked(Result("f.csv", "déposé", True)) == "f.csv — déposé"


def test_checked_echec_leve() -> None:
    with pytest.raises(RuntimeError, match="ÉCHEC MD5"):
        checked(Result("f.csv", "ÉCHEC MD5", False))


def test_filigrane_plus_ancien_jour_manquant() -> None:
    amont = {"lineE": {date(2025, 1, 1), date(2025, 1, 2)}, "lineA": {date(2025, 5, 1)}}
    aval = {"lineE": {date(2025, 1, 1)}}  # il manque le 2025-01-02 (et tout lineA)
    assert filigrane(amont, aval) == date(2025, 1, 2)


def test_filigrane_none_si_a_jour() -> None:
    jours = {"lineA": {date(2025, 5, 1)}}
    assert filigrane(jours, jours) is None


def test_pas_a_jour_quand_aucun_jour() -> None:
    res = pas(None, lambda _c: None, lambda _c, _j: [], "staging")
    assert res.ok and res.statut == "à jour" and res.label == "staging"


def test_pas_traite_le_jour_renvoye() -> None:
    vus: list[date] = []

    def traiter(_c: object, jour: date) -> list[Result]:
        vus.append(jour)
        return [Result(f"lineE {jour}", "1 ligne(s) → x", True)]

    res = pas(None, lambda _c: date(2025, 1, 1), traiter, "staging")
    assert res.ok and "ligne(s) de production" in res.statut
    assert vus == [date(2025, 1, 1)]


def test_drainer_boucle_jusqu_a_jour(capsys: pytest.CaptureFixture[str]) -> None:
    suite = iter([
        Result("2025-01-01", "1 ligne(s) de production traitée(s)", True),
        Result("staging", "à jour", True),
    ])
    code = drainer(None, lambda _c: next(suite), "Drainage test")
    assert code == 0
    assert "Drainage test" in capsys.readouterr().out
