"""
Validate that component combinations generate a valid Compose file.

Booting every combination isn't practical, but *generating* and structurally
validating them is — and that catches the bugs that matter (bad YAML, broken
references, missing volumes). For each implemented option in every layer, this
swaps it into a baseline stack, runs `composer.assemble`, then
`docker compose config -q`. It also validates the five named sample scenarios.

    python samples/validate_all.py

Exit code is non-zero if anything fails, so it doubles as a CI smoke test.
Run from the repo root.
"""

import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

import composer                                    # noqa: E402
from scenarios import SCENARIOS                    # noqa: E402

BASELINE = {
    "storage": "minio",
    "table_format": "iceberg",
    "metastore": "hive_metastore",
    "processing": "spark",
    "query_engine": "trino",
}


def _module_missing(path: str | None) -> bool:
    if path is None:          # config-only option (e.g. spark_sql) — valid
        return False
    return not os.path.exists(path.replace(".", "/") + ".py")


def _config_ok() -> bool:
    r = subprocess.run(
        ["docker", "compose", "-f", "generated/docker-compose.yml", "config", "-q"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print("    " + (r.stderr.strip().splitlines() or ["(no stderr)"])[-1])
    return r.returncode == 0


def main() -> int:
    results = {"ok": 0, "fail": 0, "skip": 0}
    failures: list[str] = []

    print("== Per-layer options (swapped into the baseline) ==")
    for layer, options in composer.MODULE_MAP.items():
        for opt, path in options.items():
            label = f"{layer}={opt}"
            if _module_missing(path):
                print(f"  SKIP {label} (not implemented)")
                results["skip"] += 1
                continue
            sel = {**BASELINE, layer: opt}
            try:
                composer.assemble(sel, stack_name=label)
                ok = _config_ok()
            except Exception as e:  # noqa: BLE001
                print(f"    {type(e).__name__}: {e}")
                ok = False
            print(f"  {'OK  ' if ok else 'FAIL'} {label}")
            results["ok" if ok else "fail"] += 1
            if not ok:
                failures.append(label)

    print("\n== Named sample scenarios ==")
    for name, scn in SCENARIOS.items():
        sel = {k: v for k, v in scn["selections"].items() if v}
        composer.assemble(sel, stack_name=name)
        ok = _config_ok()
        print(f"  {'OK  ' if ok else 'FAIL'} {name}")
        results["ok" if ok else "fail"] += 1
        if not ok:
            failures.append(name)

    print(f"\nSummary: {results['ok']} ok, {results['fail']} fail, "
          f"{results['skip']} skipped (unimplemented).")
    if failures:
        print("Failed: " + ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
