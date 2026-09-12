"""CLAUDE.md hard rule 2: benchmarks/ never imports from incidents/dev/.

The two exist to be different things - incidents/dev/ writes straight to
Postgres for a fast dev loop, the benchmark path injects into the real
replayed stream so detection latency means something. A dev trigger
reached from the benchmark would silently turn a published latency
number into a measurement of a direct database write.

Checked two ways, because one isn't enough:

1. AST imports - catches `import incidents.dev...`, `from incidents.dev
   import ...`, and literal-argument `importlib.import_module(...)` /
   `__import__(...)`. Parsing rather than grepping means a mention
   inside a docstring or comment can't fail the test, and a real import
   can't hide behind formatting.
2. Literal strings naming the path - catches the routes AST can't see:
   loading by file path, subprocess invocation, sys.path manipulation.
"""
import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_ROOTS = (REPO_ROOT / "benchmarks", REPO_ROOT / "incidents" / "benchmark")
FORBIDDEN_MODULE = "incidents.dev"
FORBIDDEN_LITERALS = ("incidents.dev", "incidents/dev", "incidents\\dev")


def benchmark_python_files():
    files = []
    for root in BENCHMARK_ROOTS:
        if root.exists():
            files.extend(sorted(root.rglob("*.py")))
    return files


def _is_forbidden(module):
    return module == FORBIDDEN_MODULE or module.startswith(FORBIDDEN_MODULE + ".")


def _dynamic_import_targets(node):
    """Literal module strings passed to importlib.import_module or
    __import__, the two dynamic forms an AST can still resolve."""
    if not isinstance(node, ast.Call):
        return []
    func = node.func
    name = None
    if isinstance(func, ast.Name):
        name = func.id
    elif isinstance(func, ast.Attribute):
        name = func.attr
    if name not in ("import_module", "__import__"):
        return []
    return [
        arg.value for arg in node.args
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
    ]


def test_benchmark_does_not_import_incidents_dev():
    violations = []
    for path in benchmark_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_forbidden(alias.name):
                        violations.append(f"{path}:{node.lineno} import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and _is_forbidden(node.module):
                    violations.append(f"{path}:{node.lineno} from {node.module} import ...")
            else:
                for target in _dynamic_import_targets(node):
                    if _is_forbidden(target):
                        violations.append(f"{path}:{node.lineno} dynamic import {target!r}")

    assert not violations, (
        "CLAUDE.md hard rule 2 violated - benchmark code imports from incidents/dev/:\n  "
        + "\n  ".join(violations)
    )


def _docstring_node_ids(tree):
    """Identities of the string nodes that are docstrings. Docstrings
    legitimately discuss this very boundary - the catalog explains why
    it is separate from the dev triggers - so they're excluded by
    identity rather than by guessing from the text."""
    ids = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            ids.add(id(body[0].value))
    return ids


def test_benchmark_does_not_reference_incidents_dev_by_path():
    violations = []
    for path in benchmark_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        docstring_ids = _docstring_node_ids(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if id(node) in docstring_ids:
                continue
            for literal in FORBIDDEN_LITERALS:
                if literal in node.value:
                    violations.append(f"{path}:{node.lineno} string {node.value!r}")

    assert not violations, (
        "benchmark code names incidents/dev/ in a string - loading it by path or "
        "subprocess would evade the import check:\n  " + "\n  ".join(violations)
    )


def test_the_check_actually_sees_benchmark_files():
    """A boundary test that silently scanned zero files would pass
    forever - exactly the failure mode DEFENSE.md #45 caught in the
    schema checks (a test passing for the wrong reason)."""
    files = benchmark_python_files()
    assert files, f"no benchmark Python files found under {[str(r) for r in BENCHMARK_ROOTS]}"
