import ast
from pathlib import Path
import logging

import database


class SecretBearingDatabaseError(RuntimeError):
    code = 11000

    def __str__(self) -> str:
        return "mongodb+srv://user:super-secret@cluster.example/private payload"


def test_database_exception_logging_never_serializes_exception_message(caplog):
    error = SecretBearingDatabaseError()

    with caplog.at_level(logging.ERROR, logger=database.__name__):
        database._log_db_exception("sensitive operation", error)

    log = caplog.text
    assert "sensitive operation failed (SecretBearingDatabaseError, code=11000)" in log
    assert "super-secret" not in log
    assert "mongodb+srv://" not in log
    assert "private payload" not in log


def test_database_warning_redaction_uses_same_safe_shape(caplog):
    error = RuntimeError("password=never-log-me")

    with caplog.at_level(logging.WARNING, logger=database.__name__):
        database._log_db_exception("index preparation", error, level=logging.WARNING)

    log = caplog.text
    assert "index preparation failed (RuntimeError)" in log
    assert "never-log-me" not in log


def test_database_module_has_no_raw_exception_logger_arguments():
    source = Path(database.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_names = {"e", "exc", "error", "last_error"}
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        owner = node.func.value
        if not (isinstance(owner, ast.Name) and owner.id == "logger"):
            continue
        if node.func.attr not in {"error", "warning", "exception", "critical"}:
            continue
        if any(
            isinstance(child, ast.Name) and child.id in forbidden_names
            for arg in node.args
            for child in ast.walk(arg)
        ):
            violations.append((node.lineno, node.func.attr))
    assert violations == []
