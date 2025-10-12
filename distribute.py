#!/usr/bin/env python3
"""
modify_sql_federated.py • 2025-06-18
-----------------------------------------------------------
Prefixes every *unqualified* table reference in each .sql file under
--input-dir with a random <catalog>.<schema> combination from a JSON/YAML
mapping.  CTE names, column aliases, and built-in functions are never
modified because we work on the parsed SQL AST (via sqlglot).

Extra behaviour
---------------
* removes "-- ..." comment lines
* strips one trailing semicolon
* writes rewritten files into --output-dir (defaults to input dir, keeping the
  original by appending "_mod.sql" if output==input)
* writes a single-line version of each query into <output>/single_line

Dependencies
------------
pip install sqlglot pyyaml      # pyyaml only when you use YAML mappings
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Dict, List

import sqlglot
from sqlglot import exp  # AST node classes

# ────────────────────────────────────────────────────────────────────────
# Optional YAML support
# ────────────────────────────────────────────────────────────────────────
try:
    import yaml  # type: ignore
except ImportError:
    yaml = None  # only needed for YAML mappings


# ────────────────────────────────────────────────────────────────────────
# Mapping loader
# ────────────────────────────────────────────────────────────────────────
def load_mapping(path: Path) -> Dict[str, List[str]]:
    """
    { "postgres1": ["db1", "db2"], "postgres2": ["db3"] }  →  same, but str-ified
    """
    if path.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:  # pragma: no cover
            sys.exit("pyyaml not installed; run: pip install pyyaml")
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        data = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(data, dict):
        sys.exit("Mapping must be a dict of {catalog: [schema, …]}")

    return {str(cat): [str(sch) for sch in schemas] for cat, schemas in data.items()}


# ────────────────────────────────────────────────────────────────────────
# Core rewriter (AST-based, using sqlglot)
# ────────────────────────────────────────────────────────────────────────
def rewrite_sql(sql: str, combos: List[str]) -> str:
    """
    Return `sql` with every **unqualified** table node prefixed by a randomly
    chosen "<catalog>.<schema>".  Works statement-by-statement so multi-stmt
    files are supported.
    """
    try:
        statements = sqlglot.parse(sql)  # list[Expression]
    except sqlglot.errors.ParseError as e:
        # If the query cannot be parsed we leave it untouched but warn once.
        print(f"⚠  sqlglot failed to parse statement — left unchanged:\n    {e}")
        return sql

    rng = random.Random()
    rng.shuffle(combos)  # randomise starting order but deterministic per run

    def random_prefix() -> tuple[str, str]:
        catalog_schema = rng.choice(combos)
        catalog, schema = catalog_schema.split(".", 1)
        return catalog, schema

    for stmt in statements:
        # 1) Collect CTE names for this statement so we never touch them.
        cte_names = {
            cte.alias_or_name.lower()
            for cte in stmt.find_all(exp.CTE)
            if cte.alias_or_name
        }

        # 2) Walk every Table node in the AST.
        for table in stmt.find_all(exp.Table):
            # Skip if already qualified.
            if table.args.get("db") or table.args.get("catalog"):
                continue
            # Skip if this table node *is* the CTE definition itself.
            if isinstance(table.parent, exp.CTE):
                continue
            # Skip if its name matches a CTE name (usage of a CTE).
            if table.name.lower() in cte_names:
                continue

            catalog, schema = random_prefix()
            table.set("catalog", catalog)
            table.set("db", schema)

    # Join statements back together (sqlglot drops trailing semicolons)
    return ";\n".join(stmt.sql(dialect="trino") for stmt in statements)  # type: ignore[arg-type]


# ────────────────────────────────────────────────────────────────────────
# Utility
# ────────────────────────────────────────────────────────────────────────
def strip_comments_and_semicolon(text: str) -> str:
    """Remove lines beginning '--' and one trailing semicolon."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    cleaned = "\n".join(lines).rstrip()
    return cleaned[:-1] if cleaned.endswith(";") else cleaned


# ────────────────────────────────────────────────────────────────────────
# CLI / main
# ────────────────────────────────────────────────────────────────────────
def main() -> None:
    p = argparse.ArgumentParser(description="Prefix unqualified tables for federated SQL")
    p.add_argument("--input-dir", required=True, type=Path, help="Directory with .sql")
    p.add_argument("--mapping", required=True, type=Path, help="JSON/YAML {catalog:[schema,…]}")
    p.add_argument(
        "--output-dir",
        type=Path,
        help="Where to put results (default: beside originals)",
    )
    args = p.parse_args()

    if not args.input_dir.is_dir():
        sys.exit(f"Input directory not found: {args.input_dir}")

    out_dir = args.output_dir or args.input_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    single_dir = out_dir / "single_line"
    single_dir.mkdir(parents=True, exist_ok=True)

    mapping = load_mapping(args.mapping)
    combos = [f"{cat}.{schema}" for cat, schemas in mapping.items() for schema in schemas]
    if not combos:
        sys.exit("Mapping contains no <catalog>.<schema> combinations to use.")

    sql_files = sorted(args.input_dir.glob("*.sql"))
    if not sql_files:
        sys.exit("No .sql files found in input directory.")

    for src in sql_files:
        original = src.read_text(encoding="utf-8")
        cleaned = strip_comments_and_semicolon(original)
        rewritten = rewrite_sql(cleaned, combos)

        dst = (
            out_dir / src.name
            if args.output_dir
            else src.with_name(src.stem + "_mod.sql")
        )
        dst.write_text(rewritten, encoding="utf-8")

        single_dst = single_dir / src.name
        single_dst.write_text(" ".join(rewritten.split()), encoding="utf-8")

        print(
            f"✔ {src.name} → "
            f"{dst.relative_to(out_dir) if args.output_dir else dst.name}"
            f" (single-line in {single_dst.relative_to(out_dir)})"
        )

    print("All done.")


if __name__ == "__main__":
    main()
