from pathlib import Path
import importlib.util


ROOT = Path(__file__).resolve().parents[3]
PYTHON_ROOT = ROOT / "wrapper" / "python"


def test_python_wrapper_uses_development_path_with_exact_version():
    cargo_toml = (PYTHON_ROOT / "Cargo.toml").read_text(encoding="utf-8")

    assert 'timslite = { path = "../..", version = "=0.1.1" }' in cargo_toml


def test_prepare_publish_rewrites_timslite_dependency(tmp_path):
    script = PYTHON_ROOT / "scripts" / "prepare_publish.py"
    spec = importlib.util.spec_from_file_location("prepare_publish", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    repo_root = tmp_path / "repo"
    wrapper_root = repo_root / "wrapper" / "python"
    wrapper_root.mkdir(parents=True)

    (repo_root / "Cargo.toml").write_text(
        '[package]\nname = "timslite"\nversion = "0.1.1"\n',
        encoding="utf-8",
    )
    (wrapper_root / "pyproject.toml").write_text(
        '[project]\nname = "timslite"\nversion = "0.1.1"\n',
        encoding="utf-8",
    )
    cargo_toml = wrapper_root / "Cargo.toml"
    cargo_toml.write_text(
        '[package]\nname = "timslite-python"\nversion = "0.1.1"\n\n'
        '[dependencies]\ntimslite = { path = "../..", version = "=0.1.1" }\n',
        encoding="utf-8",
    )

    module.prepare_publish(wrapper_root)

    assert 'timslite = { version = "=0.1.1" }' in cargo_toml.read_text(encoding="utf-8")


def test_readme_documents_pip_install_and_source_build_fallback():
    readme = (PYTHON_ROOT / "README.md").read_text(encoding="utf-8")

    assert "pip install timslite" in readme
    assert "source" in readme.lower()
    assert "crates.io" in readme


def test_release_workflow_prepares_sdist_for_crates_io_source_build():
    workflow = (ROOT / ".github" / "workflows" / "python-release.yml").read_text(encoding="utf-8")

    assert 'workflows: ["Release Rust Crate"]' in workflow
    assert "github.event.workflow_run.head_sha || github.ref" in workflow
    assert "github.event.workflow_run.event == 'release'" in workflow
    assert "python scripts/prepare_publish.py" in workflow
    assert "if cargo generate-lockfile --manifest-path Cargo.toml && cargo fetch --locked --manifest-path Cargo.toml" in workflow
    assert "cargo generate-lockfile --manifest-path Cargo.toml" in workflow
    assert "cargo fetch --locked --manifest-path Cargo.toml" in workflow
