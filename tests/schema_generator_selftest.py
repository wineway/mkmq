#!/usr/bin/env python3
"""Verify checked-in fixed Kafka codec artifacts match the generator output."""

from __future__ import annotations

import difflib
import pathlib
import re
import sys


def main() -> int:
    repo = pathlib.Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo / "tools"))

    import generate_kafka_schemas as generator  # pylint: disable=import-error,import-outside-toplevel

    schema_source = (repo / "src/generated/kafka_schema.cpp").read_text(encoding="utf-8")
    match = re.search(r'return "([0-9a-f]+)";', schema_source)
    if match is None:
        raise RuntimeError("could not find generated Kafka schema source commit")
    commit = match.group(1)

    checks = [
        (
            "include/mkmq/generated/kafka_codec.hpp",
            generator.render_codec_header(commit),
        ),
        (
            "src/generated/kafka_codec.cpp",
            generator.render_codec_source(commit),
        ),
    ]

    failed = False
    for rel_path, rendered in checks:
        path = repo / rel_path
        checked_in = path.read_text(encoding="utf-8")
        if checked_in == rendered:
            continue
        failed = True
        print(f"{rel_path} does not match tools/generate_kafka_schemas.py output", file=sys.stderr)
        diff = difflib.unified_diff(
            checked_in.splitlines(True),
            rendered.splitlines(True),
            fromfile=f"checked-in/{rel_path}",
            tofile=f"generated/{rel_path}",
        )
        sys.stderr.writelines(diff)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
