"""
Run one sample scenario end to end.

    python samples/run_sample.py <name>          # generate + start + show status
    python samples/run_sample.py <name> --down   # stop it and remove volumes
    python samples/run_sample.py --list          # list scenario names

Order of operations (also what a CI test would do):
  1. assemble the selection  -> generated/docker-compose.yml
  2. docker compose up -d
  3. poll until services are healthy
  4. print the service endpoints

Run from the repo root. Works the same on Windows, Linux and macOS (everything
goes through `docker compose`).
"""

import os
import subprocess
import sys
import time

# Make the repo root importable and the working dir, so composer writes to
# generated/ and relative bind mounts resolve correctly.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

from composer import assemble                      # noqa: E402
from scenarios import SCENARIOS                    # noqa: E402  (same dir)
import setup                                       # noqa: E402  (endpoint printer)

COMPOSE = ["docker", "compose", "-f", "generated/docker-compose.yml"]


def _dc(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([*COMPOSE, *args])


def up(name: str) -> int:
    scn = SCENARIOS[name]
    print(f"\n=== {name} ===\n{scn['description']}\n")
    sel = {k: v for k, v in scn["selections"].items() if v}

    print("[1/4] Generating stack...")
    assemble(sel, stack_name=name)

    # Start from a clean slate: samples are independent, reproducible scenarios,
    # so clear any previous platform containers/volumes first. (All generated
    # stacks share the 'open-data-platform' Compose project, so leftover data —
    # e.g. a Debezium replication slot — would otherwise break a different one.)
    print("[2/4] Resetting any previous stack, then starting...")
    _dc("down", "-v")
    if _dc("up", "-d").returncode != 0:
        print("docker compose up failed — see output above.")
        return 1

    print("[3/4] Waiting for services to become healthy (up to 5 min)...")
    deadline = time.time() + 300
    while time.time() < deadline:
        ps = subprocess.run([*COMPOSE, "ps", "-a", "--format", "{{.Service}} {{.Status}}"],
                            capture_output=True, text=True).stdout.strip().splitlines()
        starting = [l for l in ps if "health: starting" in l or "Restarting" in l]
        bad = [l for l in ps if "Exited (1" in l or "Exited (2" in l]
        if bad:
            print("Some services failed:\n  " + "\n  ".join(bad))
            return 1
        if not starting:
            break
        time.sleep(10)

    print("[4/4] Status:")
    _dc("ps")
    setup.print_endpoints("generated")
    print(f"\nTear down with:  python samples/run_sample.py {name} --down")
    return 0


def down(name: str) -> int:
    scn = SCENARIOS[name]
    assemble({k: v for k, v in scn["selections"].items() if v}, stack_name=name)
    return _dc("down", "-v").returncode


def main() -> int:
    args = sys.argv[1:]
    if not args or args[0] in ("--list", "-l"):
        print("Available scenarios:")
        for n, s in SCENARIOS.items():
            print(f"  {n:22s} {s['description']}")
        return 0
    name = args[0]
    if name not in SCENARIOS:
        print(f"Unknown scenario '{name}'. Use --list to see options.")
        return 2
    return down(name) if "--down" in args else up(name)


if __name__ == "__main__":
    raise SystemExit(main())
