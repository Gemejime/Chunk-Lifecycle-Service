#!/usr/bin/env python3
"""
scripts/sync_env_to_deployments.py

Synchronizes and validates environment configuration between .env.prod (single source of truth)
and deployments/helm/values.yaml.

Usage:
    python scripts/sync_env_to_deployments.py --sync
    python scripts/sync_env_to_deployments.py --check-only
"""

import argparse
import os
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install via 'pip install pyyaml'.", file=sys.stderr)
    sys.exit(1)

# Ensure clean UTF-8 stdout/stderr on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Patterns identifying sensitive credentials that must never be written to values.yaml
SENSITIVE_PATTERNS = ("PASSWORD", "SECRET", "TOKEN", "KEY")


def is_sensitive(key: str) -> bool:
    """Return True if the key contains any sensitive credential keywords."""
    upper = key.upper()
    return any(p in upper for p in SENSITIVE_PATTERNS)


def parse_env_file(filepath: Path) -> dict[str, str]:
    """Parse an environment file into a dictionary of string key-values."""
    if not filepath.exists():
        raise FileNotFoundError(f"Environment file not found: {filepath}")

    env: dict[str, str] = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in stripped:
                continue

            key, val = stripped.split("=", 1)
            key = key.strip()
            val = val.strip()

            # Handle quoted values
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            elif " #" in val:
                val = val.split(" #", 1)[0].strip()

            env[key] = val

    return env


def get_syncable_env(env_vars: dict[str, str]) -> dict[str, str]:
    """Filter out sensitive keys so credentials are never written to values.yaml."""
    return {k: v for k, v in env_vars.items() if not is_sensitive(k)}


def resolve_path(given_path: str, repo_root: Path) -> Path:
    """Resolve a file path relative to current working directory or repository root."""
    p = Path(given_path)
    if p.is_absolute() and p.exists():
        return p
    cwd_path = Path.cwd() / p
    if cwd_path.exists():
        return cwd_path
    root_path = repo_root / p
    return root_path


def check_drift(env_path: Path, values_path: Path) -> int:
    """
    Verify zero drift between .env.prod and deployments/helm/values.yaml.
    Exits with code 1 if drift detected, code 0 if clean.
    """
    if not env_path.exists():
        print(f"[ERROR] Environment file not found: {env_path}", file=sys.stderr)
        return 1
    if not values_path.exists():
        print(f"[ERROR] Values file not found: {values_path}", file=sys.stderr)
        return 1

    prod_raw = parse_env_file(env_path)
    syncable_prod = get_syncable_env(prod_raw)

    with open(values_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    raw_env_section = data.get("env") or {}
    # Ignore secretName and keys with None values
    values_env = {
        str(k): str(v)
        for k, v in raw_env_section.items()
        if k != "secretName" and v is not None
    }

    missing_in_values = [k for k in syncable_prod if k not in values_env]
    extra_in_values = [k for k in values_env if k not in syncable_prod]
    mismatched = [
        (k, syncable_prod[k], values_env[k])
        for k in syncable_prod
        if k in values_env and syncable_prod[k] != values_env[k]
    ]

    if missing_in_values or extra_in_values or mismatched:
        print("=" * 80, file=sys.stderr)
        print("[DRIFT DETECTED] Helm values.yaml is out of sync with .env.prod!", file=sys.stderr)
        print(f"Source of truth: {env_path}", file=sys.stderr)
        print(f"Target Helm values: {values_path}", file=sys.stderr)
        print("-" * 80, file=sys.stderr)
        if missing_in_values:
            print(f"  Missing in values.yaml ({len(missing_in_values)}): {', '.join(missing_in_values)}", file=sys.stderr)
        if extra_in_values:
            print(f"  Unexpected in values.yaml ({len(extra_in_values)}): {', '.join(extra_in_values)}", file=sys.stderr)
        if mismatched:
            print(f"  Value mismatches ({len(mismatched)}):", file=sys.stderr)
            for k, prod_v, val_v in mismatched:
                print(f"    - {k}: .env.prod={prod_v!r} vs values.yaml={val_v!r}", file=sys.stderr)
        print("-" * 80, file=sys.stderr)
        print("To synchronize, run:", file=sys.stderr)
        print("    python scripts/sync_env_to_deployments.py --sync", file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        return 1

    print(f"[OK] Zero drift: {values_path} is fully synchronized with {env_path}.")
    return 0


def sync_env(env_path: Path, values_path: Path) -> int:
    """
    Update deployments/helm/values.yaml from .env.prod.
    Preserves secretName, comments, line feeds, indentation, and all top-level Helm blocks.
    """
    if not env_path.exists():
        print(f"[ERROR] Environment file not found: {env_path}", file=sys.stderr)
        return 1
    if not values_path.exists():
        print(f"[ERROR] Values file not found: {values_path}", file=sys.stderr)
        return 1

    prod_raw = parse_env_file(env_path)
    syncable_prod = get_syncable_env(prod_raw)

    with open(values_path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines(keepends=True)

    # Locate top-level 'env:' block
    env_start = -1
    env_end = -1
    existing_secret_name = None

    re_top_level = re.compile(r"^[a-zA-Z0-9_-]+:\s*(?:#.*)?$")
    re_secret_name = re.compile(r"^\s*secretName:\s*(.*)$")

    for i, line in enumerate(lines):
        stripped = line.rstrip("\r\n")
        # Check for top-level env:
        if stripped.startswith("env:") and (len(stripped) == 4 or stripped[4:].strip().startswith("#")):
            env_start = i
            continue

        if env_start != -1 and env_end == -1:
            # Check for existing secretName
            m_sec = re_secret_name.match(stripped)
            if m_sec:
                existing_secret_name = m_sec.group(1).strip()

            # Check if we hit the next top-level unindented block
            if line and not line.startswith(" ") and not line.startswith("\t") and not stripped.startswith("#"):
                if re_top_level.match(stripped) or (":" in stripped and not stripped.startswith("-")):
                    env_end = i
                    break

    if env_start == -1:
        print("[ERROR] Top-level 'env:' block not found in values.yaml", file=sys.stderr)
        return 1

    if env_end == -1:
        env_end = len(lines)

    # Construct replacement lines for env:
    # Detect line-ending style (CRLF vs LF)
    newline = "\r\n" if lines and lines[0].endswith("\r\n") else "\n"

    new_env_lines = [f"env:{newline}"]
    if existing_secret_name is not None:
        new_env_lines.append(f"  secretName: {existing_secret_name}{newline}")

    for k in sorted(syncable_prod.keys()):
        val = syncable_prod[k]
        # Quote string values cleanly
        escaped_val = val.replace('"', '\\"')
        new_env_lines.append(f'  {k}: "{escaped_val}"{newline}')

    # Always ensure a trailing newline separates env: from subsequent top-level blocks
    new_env_lines.append(newline)

    updated_lines = lines[:env_start] + new_env_lines + lines[env_end:]
    updated_content = "".join(updated_lines)

    with open(values_path, "w", encoding="utf-8") as f:
        f.write(updated_content)

    sensitive_excluded = [k for k in prod_raw if is_sensitive(k)]
    print(f"[SUCCESS] Synchronized {len(syncable_prod)} environment variables from {env_path} to {values_path}.")
    if existing_secret_name:
        print(f"  - Preserved secretName: {existing_secret_name}")
    if sensitive_excluded:
        print(f"  - Excluded {len(sensitive_excluded)} sensitive keys: {', '.join(sensitive_excluded)}")

    return 0


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent

    parser = argparse.ArgumentParser(
        description="Synchronize and validate .env.prod to deployments/helm/values.yaml"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sync",
        action="store_true",
        help="Updates deployments/helm/values.yaml from .env.prod",
    )
    group.add_argument(
        "--check-only",
        action="store_true",
        help="Verifies zero drift between .env.prod and deployments/helm/values.yaml (exits with code 1 if drift detected, code 0 if clean)",
    )
    parser.add_argument(
        "--env-file",
        default=".env.prod",
        help="Path to production environment file (default: .env.prod)",
    )
    parser.add_argument(
        "--values-file",
        default="deployments/helm/values.yaml",
        help="Path to Helm values.yaml (default: deployments/helm/values.yaml)",
    )

    args = parser.parse_args()

    env_path = resolve_path(args.env_file, repo_root)
    values_path = resolve_path(args.values_file, repo_root)

    if args.sync:
        sys.exit(sync_env(env_path, values_path))
    elif args.check_only:
        sys.exit(check_drift(env_path, values_path))


if __name__ == "__main__":
    main()
