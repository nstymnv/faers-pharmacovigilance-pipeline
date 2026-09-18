import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[2]

# FAERS quarterly archives follow a stable URL pattern, so quarters are addressed
# directly. The FAERS index page renders its file list client-side, which is why
# scraping it returned nothing.
FAERS_ZIP_URL_TEMPLATE = "https://fis.fda.gov/content/Exports/faers_xml_{year}q{quarter}.zip"
FAERS_HOST = "fis.fda.gov"
YEARS_TO_DOWNLOAD = 5

EXTRACTION_SCHEMA = "extraction"
RAW_STAGE = f"{EXTRACTION_SCHEMA}.faers_raw"
CODE_STAGE = f"{EXTRACTION_SCHEMA}.code"
INGESTION_LOG_TABLE = f"{EXTRACTION_SCHEMA}.ingestion_log"

RAW_SCHEMA = "raw"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set. Copy .env.example to .env and fill it in.")

    return value


def snowflake_private_key_path() -> Path:
    # A relative path resolves against the repo root rather than the process's cwd,
    # so the pipeline behaves the same however it is invoked.
    key_path = Path(require_env("SNOWFLAKE_PRIVATE_KEY_PATH"))
    if not key_path.is_absolute():
        key_path = REPO_ROOT / key_path

    if not key_path.is_file():
        raise RuntimeError(f"Snowflake private key not found at {key_path}")

    return key_path


def build_connection_parameters() -> dict[str, str]:
    """Snowflake connection parameters for Snowpark and the Python connector.

    The connector reads the key-pair file itself, so no PEM handling is needed here.
    """
    return {
        "account": require_env("SNOWFLAKE_ACCOUNT"),
        "user": require_env("SNOWFLAKE_USER"),
        "private_key_file": str(snowflake_private_key_path()),
        "warehouse": require_env("SNOWFLAKE_WAREHOUSE"),
        "database": require_env("SNOWFLAKE_DATABASE"),
        "schema": require_env("SNOWFLAKE_SCHEMA"),
        "role": require_env("SNOWFLAKE_ROLE"),
    }


def quarter_key(year: int, quarter: int) -> str:
    return f"{year}q{quarter}"


def faers_zip_url(year: int, quarter: int) -> str:
    return FAERS_ZIP_URL_TEMPLATE.format(year=year, quarter=quarter)


def stage_xml_prefix(year: int, quarter: int) -> str:
    return f"@{RAW_STAGE}/xml/year={year}/quarter={quarter}"
