"""Stored procedure handler: download one FAERS quarter into the raw stage.

Runs inside Snowflake with an external access integration for fis.fda.gov. The
whole quarter is streamed — the ZIP to local scratch, then each XML member out to
the stage one at a time — so peak disk stays around one member rather than the
~2 GB an unpacked quarter would need.
"""

import os
import shutil
import time
import zipfile
from datetime import datetime, timezone

import requests

FAERS_ZIP_URL_TEMPLATE = "https://fis.fda.gov/content/Exports/faers_xml_{year}q{quarter}.zip"
RAW_STAGE = "@extraction.faers_raw"
INGESTION_LOG_TABLE = "extraction.ingestion_log"

DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = (30, 300)  # (connect, read)


def _quarter_key(year: int, quarter: int) -> str:
    return f"{year}q{quarter}"


def _log(session, quarter_key, source_url, status, files, size, started_at, error=None):
    session.sql(
        f"insert into {INGESTION_LOG_TABLE}"
        " (quarter_key, source_url, status, files_staged, bytes_staged,"
        "  started_at, ended_at, error_message)"
        # nullif: Snowpark binds a Python None as the literal string 'None'.
        " select ?, ?, ?, ?, ?, ?, ?, nullif(?, '')",
        params=[
            quarter_key,
            source_url,
            status,
            files,
            size,
            started_at,
            datetime.now(timezone.utc),
            error or "",
        ],
    ).collect()


def _already_succeeded(session, quarter_key: str) -> bool:
    rows = session.sql(
        f"select count(*) from {INGESTION_LOG_TABLE}"
        " where quarter_key = ? and status = 'SUCCEEDED'",
        params=[quarter_key],
    ).collect()

    return rows[0][0] > 0


def _download_zip(url: str, destination: str) -> int:
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        if response.status_code == 404:
            raise FileNotFoundError(f"FDA has not published {url}")
        response.raise_for_status()

        with open(destination, "wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                handle.write(chunk)

    return os.path.getsize(destination)


def _stage_members(session, zip_path: str, work_dir: str, year: int, quarter: int):
    """Extract XML and deleted-case members one at a time and PUT each to the stage.

    Members are addressed by name rather than via extractall(), which would follow
    whatever paths the archive claims.
    """
    xml_prefix = f"{RAW_STAGE}/xml/year={year}/quarter={quarter}"
    deleted_prefix = f"{RAW_STAGE}/deleted/year={year}/quarter={quarter}"

    staged_files = 0
    staged_bytes = 0

    with zipfile.ZipFile(zip_path) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue

            name = info.filename.lower()
            if name.endswith(".xml"):
                stage_prefix = xml_prefix
            elif "deleted" in name and name.endswith(".txt"):
                stage_prefix = deleted_prefix
            else:
                continue

            local_name = os.path.basename(info.filename)
            local_path = os.path.join(work_dir, local_name)

            with archive.open(info) as source, open(local_path, "wb") as target:
                shutil.copyfileobj(source, target, DOWNLOAD_CHUNK_BYTES)

            # auto_compress=False: Snowpark Connect cannot read compressed XML.
            session.file.put(
                f"file://{local_path}",
                stage_prefix,
                auto_compress=False,
                overwrite=True,
            )

            staged_files += 1
            staged_bytes += os.path.getsize(local_path)
            os.remove(local_path)

    return staged_files, staged_bytes


def run(session, year: int, quarter: int, force: bool = False) -> dict:
    quarter_key = _quarter_key(year, quarter)
    source_url = FAERS_ZIP_URL_TEMPLATE.format(year=year, quarter=quarter)
    started_at = datetime.now(timezone.utc)

    if not force and _already_succeeded(session, quarter_key):
        return {"quarter": quarter_key, "status": "SKIPPED", "reason": "already ingested"}

    work_dir = f"/tmp/faers_{quarter_key}_{int(time.time())}"
    os.makedirs(work_dir, exist_ok=True)
    zip_path = os.path.join(work_dir, f"faers_xml_{quarter_key}.zip")

    try:
        zip_bytes = _download_zip(source_url, zip_path)
        files, staged_bytes = _stage_members(session, zip_path, work_dir, year, quarter)

        if files == 0:
            raise RuntimeError(f"no XML members found in {source_url}")

        _log(session, quarter_key, source_url, "SUCCEEDED", files, staged_bytes, started_at)

        return {
            "quarter": quarter_key,
            "status": "SUCCEEDED",
            "zip_bytes": zip_bytes,
            "files_staged": files,
            "bytes_staged": staged_bytes,
        }

    except Exception as exc:
        _log(session, quarter_key, source_url, "FAILED", 0, 0, started_at, str(exc)[:1000])
        raise

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
