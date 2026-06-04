from __future__ import annotations

import pytest

from datalake.runner import Result, run


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
