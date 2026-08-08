#!/usr/bin/env python3
"""backlog #96: event contract validation.

Every topic's real wire shape is described by a JSON Schema in
schemas/*.schema.json. This is deliberately the "lightest" option named
in the AC (shared versioned JSON Schemas + CI contract tests) over
Apicurio/Confluent Schema Registry -- no new runtime component to
operate, and the schema files themselves are the same kind of
plain-JSON artifact this project already treats as the real contract
(ADR 0007's "agree on the JSON shape, not a shared Java type").

Each topic needs a `<topic>.valid.json` example (a real, representative
payload that must pass) and a `<topic>.invalid.json` example (a real,
plausible drift shape -- a renamed field, an out-of-range value, an
enum value that no longer matches -- that must fail). The AC's own bar
("a deliberately-broken contract proven to fail CI before merge") is
this script failing loudly if either check doesn't behave as expected,
not just "the schema file exists."
"""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"
EXAMPLES_DIR = SCHEMAS_DIR / "examples"


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def check_topic(schema_path: Path) -> list[str]:
    topic = schema_path.stem.removesuffix(".schema")
    errors = []

    schema = load(schema_path)
    try:
        Draft202012Validator.check_schema(schema)
    except ValidationError as e:
        errors.append(f"{topic}: schema itself is not valid JSON Schema: {e.message}")
        return errors

    validator = Draft202012Validator(schema)

    valid_example = EXAMPLES_DIR / f"{topic}.valid.json"
    invalid_example = EXAMPLES_DIR / f"{topic}.invalid.json"

    if not valid_example.exists():
        errors.append(f"{topic}: missing {valid_example.relative_to(SCHEMAS_DIR.parent)}")
    else:
        instance = load(valid_example)
        problems = sorted(validator.iter_errors(instance), key=lambda e: e.path)
        if problems:
            detail = "; ".join(p.message for p in problems)
            errors.append(f"{topic}: the golden valid example fails its own schema: {detail}")

    if not invalid_example.exists():
        errors.append(f"{topic}: missing {invalid_example.relative_to(SCHEMAS_DIR.parent)}")
    else:
        instance = load(invalid_example)
        problems = list(validator.iter_errors(instance))
        if not problems:
            errors.append(
                f"{topic}: the deliberately-broken example passed validation -- "
                f"the schema has no teeth against this real drift shape"
            )

    return errors


def main() -> None:
    schema_files = sorted(SCHEMAS_DIR.glob("*.schema.json"))
    if not schema_files:
        print(f"No *.schema.json files found under {SCHEMAS_DIR} -- nothing to check.", file=sys.stderr)
        sys.exit(1)

    all_errors = []
    for schema_path in schema_files:
        all_errors.extend(check_topic(schema_path))

    if all_errors:
        for e in all_errors:
            print(f"::error::{e}", file=sys.stderr)
        print(f"\n{len(all_errors)} contract validation error(s) found (backlog #96).", file=sys.stderr)
        sys.exit(1)

    print(f"All {len(schema_files)} event contracts OK: schema valid, golden example passes, broken example fails.")


if __name__ == "__main__":
    main()
