"""
Pre-commit configuration for git.

This file was created by precommit (https://github.com/iafisher/precommit).
You are welcome to edit it yourself to customize your pre-commit hook.
"""
from iprecommit import checks


def init(precommit):
    precommit.check(checks.NoStagedAndUnstagedChanges())
    precommit.check(checks.NoWhitespaceInFilePath())
    precommit.check(checks.DoNotSubmit())

    # Check Python format with black.
    precommit.check(checks.PythonFormat(exclude=["docs/conf.py"]))

    # Lint Python code with flake8.
    precommit.check(checks.PythonLint(exclude=["docs/conf.py"]))

    # Check the order of Python imports with isort.
    precommit.check(checks.PythonImportOrder(exclude=["docs/conf.py"]))

    # Check that requirements.txt matches pip freeze.
    precommit.check(checks.PipFreeze(venv=".venv"))

    # Check Python static type annotations with mypy.
    # precommit.check(checks.PythonTypes())

    # Lint JavaScript code with ESLint.
    precommit.check(checks.JavaScriptLint())

    # Check Rust format with rustfmt.
    precommit.check(checks.RustFormat())

    # Run the test suite.
    precommit.check(checks.Command("UnitTests", ["./do", "test"]))

    precommit.check(
        checks.Command(
            "PythonTypes", [".venv/bin/mypy", "--ignore-missing-imports", "isqlite"]
        )
    )
