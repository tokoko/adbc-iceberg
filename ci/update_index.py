#!/usr/bin/env python3
"""Merge a new release into the repository's dbc registry index.

The index is an ordinary dbc `index.yaml` -- the same document Columnar's CDN
serves -- attached as an asset to every GitHub release. Package URLs are
absolute, pointing at each version's own release, because GitHub only serves
assets under /releases/download/<tag>/ (and /releases/latest/download/ resolves
to the newest release only).

dbc uses absolute package URLs verbatim, so no client-side change is needed:
point a registry at <repo>/releases/latest/download and it will fetch
<that>/index.yaml.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

DOWNLOAD_URL = "https://github.com/{repo}/releases/download/{tag}/{asset}"


def version_key(version: str) -> tuple:
    """Sort key for a semver-ish string; prereleases sort below their release."""
    core, _, pre = version.lstrip("v").partition("-")
    parts = []
    for chunk in core.split("."):
        m = re.match(r"^(\d+)", chunk)
        parts.append(int(m.group(1)) if m else 0)
    parts += [0] * (3 - len(parts))
    # A release outranks any prerelease of the same core version.
    return (*parts[:3], 1 if not pre else 0, pre)


def load_index(path: Path | None) -> dict:
    if path and path.is_file() and path.stat().st_size > 0:
        data = yaml.safe_load(path.read_text()) or {}
        if isinstance(data, dict):
            return data
    return {}


def find_driver(index: dict, driver_path: str) -> dict | None:
    for entry in index.get("drivers", []):
        if entry.get("path") == driver_path:
            return entry
    return None


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--existing", type=Path, help="previous index.yaml, if any")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--repo", required=True, help="owner/repo")
    p.add_argument("--tag", required=True, help="git tag of this release")
    p.add_argument("--version", required=True, help="version, e.g. v0.1.0")
    p.add_argument("--driver", required=True, help="dbc driver path, e.g. iceberg")
    p.add_argument("--registry-name", default="adbc-iceberg")
    p.add_argument("--title", default="ADBC Iceberg Driver")
    p.add_argument("--description", default="An ADBC driver for Apache Iceberg tables")
    p.add_argument("--license", default="")
    p.add_argument("--url", action="append", default=[], help="project URL (repeatable)")
    p.add_argument("--docs-url", default="")
    p.add_argument(
        "--package",
        action="append",
        required=True,
        metavar="PLATFORM=ASSET",
        help="platform tuple and release asset name, e.g. linux_amd64=iceberg_linux_amd64_v0.1.0.tar.gz",
    )
    args = p.parse_args()

    packages = []
    for spec in args.package:
        platform, _, asset = spec.partition("=")
        if not platform or not asset:
            raise SystemExit(f"--package expects PLATFORM=ASSET, got {spec!r}")
        packages.append(
            {
                "platform": platform,
                "url": DOWNLOAD_URL.format(repo=args.repo, tag=args.tag, asset=asset),
            }
        )
    packages.sort(key=lambda pkg: pkg["platform"])

    index = load_index(args.existing)
    index.setdefault("name", args.registry_name)
    index.setdefault("drivers", [])

    driver = find_driver(index, args.driver)
    if driver is None:
        driver = {"name": args.title, "path": args.driver, "pkginfo": []}
        if args.description:
            driver["description"] = args.description
        if args.license:
            driver["license"] = args.license
        if args.url:
            driver["urls"] = args.url
        if args.docs_url:
            driver["docs_url"] = args.docs_url
        # Keep a stable field order matching the adbc-drivers convention.
        order = ["name", "description", "license", "path", "urls", "docs_url", "pkginfo"]
        driver = {k: driver[k] for k in order if k in driver}
        index["drivers"].append(driver)

    pkginfo = driver.setdefault("pkginfo", [])
    # Re-releasing the same version replaces its entry rather than duplicating it.
    pkginfo = [e for e in pkginfo if e.get("version") != args.version]
    pkginfo.append({"version": args.version, "packages": packages})
    pkginfo.sort(key=lambda e: version_key(str(e.get("version", ""))), reverse=True)
    driver["pkginfo"] = pkginfo

    index["drivers"].sort(key=lambda d: d.get("path", ""))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(yaml.safe_dump(index, sort_keys=False, default_flow_style=False))
    print(f"wrote {args.out} ({len(pkginfo)} version(s) of {args.driver})")


if __name__ == "__main__":
    main()
