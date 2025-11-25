from types import SimpleNamespace
import builtins

from src import report


def test_kaleido_available_false_when_import_fails(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "kaleido":
            raise ModuleNotFoundError
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.setattr(report.pio, "kaleido", None, raising=False)

    assert report.kaleido_available() is False


def test_kaleido_available_true_with_stub(monkeypatch):
    real_import = builtins.__import__
    dummy_mod = object()

    def fake_import(name, *args, **kwargs):
        if name == "kaleido":
            return dummy_mod
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.setattr(report.pio, "kaleido", SimpleNamespace(scope="dummy"), raising=False)

    assert report.kaleido_available() is True
