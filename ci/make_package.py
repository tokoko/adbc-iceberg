#!/usr/bin/env python3
"""Package a built driver shared library as a dbc-installable tarball.

dbc requires the archive to be a *flat* gzipped tar (no directory entries)
containing a top-level file named MANIFEST, plus the shared library itself.
See https://github.com/columnar-tech/dbc -- config.InflateTarball rejects any
directory entry, and config.InstallDriver locates the library via [Files] driver.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import tarfile
from pathlib import Path

# Keys dbc reads out of MANIFEST. Only `name` and `version` are strictly
# required, but `[Files] driver` is what tells dbc which file in the archive is
# the shared library, and `[Driver] entrypoint` is required whenever the driver
# does not export the default `AdbcDriverInit` symbol.
MANIFEST_TEMPLATE = """\
name = "{title}"
description = "{description}"
publisher = "{publisher}"
version = "{version}"

[ADBC]
version = "{adbc_version}"

[Driver]
entrypoint = "{entrypoint}"

[Files]
driver = "{lib_name}"
"""


def build_manifest(args: argparse.Namespace, lib_name: str) -> str:
    return MANIFEST_TEMPLATE.format(
        title=args.title,
        description=args.description,
        publisher=args.publisher,
        version=args.version,
        adbc_version=args.adbc_version,
        entrypoint=args.entrypoint,
        lib_name=lib_name,
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--lib", required=True, type=Path, help="built shared library")
    p.add_argument("--driver", required=True, help="dbc driver path, e.g. iceberg")
    p.add_argument("--version", required=True, help="version, e.g. v0.1.0")
    p.add_argument("--platform", required=True, help="dbc platform tuple, e.g. linux_amd64")
    p.add_argument("--outdir", default="dist", type=Path)
    p.add_argument("--title", default="ADBC Iceberg Driver")
    p.add_argument("--description", default="An ADBC driver for Apache Iceberg tables")
    p.add_argument("--publisher", default="tokoko")
    p.add_argument("--adbc-version", default="1.1.0")
    p.add_argument(
        "--entrypoint",
        default="AdbcDriverIcebergInit",
        help="driver init symbol; the Rust driver exports AdbcDriverIcebergInit",
    )
    p.add_argument("--extra", type=Path, action="append", default=[],
                   help="additional file to include flat in the archive (e.g. LICENSE)")
    args = p.parse_args()

    if not args.lib.is_file():
        raise SystemExit(f"shared library not found: {args.lib}")

    args.outdir.mkdir(parents=True, exist_ok=True)
    lib_name = args.lib.name

    manifest_path = args.outdir / "MANIFEST"
    manifest_path.write_text(build_manifest(args, lib_name))

    asset = f"{args.driver}_{args.platform}_{args.version}.tar.gz"
    tarball = args.outdir / asset

    # Flat archive: every arcname is a bare filename, and we never add the
    # directory itself, so no tar.TypeDir entries are produced.
    with tarfile.open(tarball, "w:gz") as tf:
        tf.add(manifest_path, arcname="MANIFEST")
        tf.add(args.lib, arcname=lib_name)
        for extra in args.extra:
            if extra.is_file():
                tf.add(extra, arcname=extra.name)

    manifest_path.unlink()

    digest = hashlib.sha256(tarball.read_bytes()).hexdigest()
    size = tarball.stat().st_size
    print(f"{tarball}  {size} bytes  sha256:{digest}")

    if out := os.environ.get("GITHUB_OUTPUT"):
        with open(out, "a") as fh:
            fh.write(f"asset={asset}\n")
            fh.write(f"sha256={digest}\n")


if __name__ == "__main__":
    main()
