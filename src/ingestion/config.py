import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[2]

FAERS_INDEX_URL = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
YEARS_TO_DOWNLOAD = 5
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"


def load_snowflake_private_key(path: str | None) -> str:
    if not path:
        raise RuntimeError("SNOWFLAKE_PRIVATE_KEY_PATH is not set")

    key_path = Path(path)
    if not key_path.is_absolute():
        key_path = REPO_ROOT / key_path

    private_key = key_path.read_text()
    return re.sub(r"-*(BEGIN|END) PRIVATE KEY-*\n", "", private_key).replace("\n", "")


def build_sf_options() -> dict[str, str | None]:
    return {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "pem_private_key": load_snowflake_private_key(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    }
