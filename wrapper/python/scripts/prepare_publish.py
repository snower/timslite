from __future__ import annotations

import re
from pathlib import Path


VERSION_RE = re.compile(r'^\s*version\s*=\s*"([^"]+)"\s*$', re.MULTILINE)
TIMSLITE_DEP_RE = re.compile(r'^timslite\s*=\s*\{[^}]*\}\s*$', re.MULTILINE)


def read_version(text: str, file_name: str) -> str:
    match = VERSION_RE.search(text)
    if not match:
        raise RuntimeError(f"{file_name} does not contain a version field")
    return match.group(1)


def prepare_publish(wrapper_root: Path | None = None) -> None:
    wrapper_root = (wrapper_root or Path(__file__).resolve().parents[1]).resolve()
    repo_root = wrapper_root.parent.parent

    root_cargo_toml = (repo_root / "Cargo.toml").read_text(encoding="utf-8")
    wrapper_cargo_path = wrapper_root / "Cargo.toml"
    wrapper_cargo_toml = wrapper_cargo_path.read_text(encoding="utf-8")
    pyproject_toml = (wrapper_root / "pyproject.toml").read_text(encoding="utf-8")

    root_version = read_version(root_cargo_toml, "root Cargo.toml")
    wrapper_version = read_version(wrapper_cargo_toml, "wrapper/python/Cargo.toml")
    project_version = read_version(pyproject_toml, "wrapper/python/pyproject.toml")

    if len({root_version, wrapper_version, project_version}) != 1:
        raise RuntimeError(
            "Version mismatch: "
            f"Cargo.toml={root_version}, "
            f"wrapper/python/Cargo.toml={wrapper_version}, "
            f"wrapper/python/pyproject.toml={project_version}"
        )

    replacement = f'timslite = {{ version = "={project_version}" }}'
    updated = TIMSLITE_DEP_RE.sub(replacement, wrapper_cargo_toml, count=1)
    if updated == wrapper_cargo_toml:
        raise RuntimeError("Could not rewrite timslite dependency in wrapper/python/Cargo.toml")

    wrapper_cargo_path.write_text(updated, encoding="utf-8")
    print(f'Prepared PyPI source distribution with timslite = "={project_version}"')


if __name__ == "__main__":
    prepare_publish()
