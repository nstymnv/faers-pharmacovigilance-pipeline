import pytest

from faers_ingestion import config


def test_quarter_addressing_is_consistent():
    assert config.quarter_key(2021, 3) == "2021q3"
    assert config.faers_zip_url(2021, 3) == (
        "https://fis.fda.gov/content/Exports/faers_xml_2021q3.zip"
    )
    assert config.stage_xml_prefix(2021, 3) == "@extraction.faers_raw/xml/year=2021/quarter=3"


def test_relative_key_path_resolves_against_repo_root(monkeypatch, tmp_path):
    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    (key_dir / "rsa_key.p8").write_text("not a real key")
    monkeypatch.setattr(config, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/rsa_key.p8")

    assert config.snowflake_private_key_path() == key_dir / "rsa_key.p8"


def test_missing_key_file_fails_with_its_path(monkeypatch, tmp_path):
    missing = tmp_path / "missing.p8"
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", str(missing))

    with pytest.raises(RuntimeError, match="missing.p8"):
        config.snowflake_private_key_path()


def test_unset_variable_names_itself(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)

    with pytest.raises(RuntimeError, match="SNOWFLAKE_ACCOUNT"):
        config.require_env("SNOWFLAKE_ACCOUNT")
