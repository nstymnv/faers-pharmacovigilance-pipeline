"""Stored procedure handler: download one FAERS quarter into the raw stage.

Runs inside Snowflake with an external access integration for fis.fda.gov. The
whole quarter is streamed — the ZIP to local scratch, then each XML member out to
the stage one at a time — so peak disk stays around one member rather than the
~2 GB an unpacked quarter would need.
"""

import os
import shutil
import struct
import time
import zipfile
import zlib
from datetime import datetime, timezone

import requests
from stream_inflate import stream_inflate64

FAERS_ZIP_URL_TEMPLATE = "https://fis.fda.gov/content/Exports/faers_xml_{year}q{quarter}.zip"
RAW_STAGE = "@extraction.faers_raw"
INGESTION_LOG_TABLE = "extraction.ingestion_log"

DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = (30, 300)  # (connect, read)

# ZIP compression method 9. FDA packed some quarters' XML with it (2023q3,
# 2024q1, 2024q4), and Python's zipfile cannot read it.
DEFLATE64 = 9
LOCAL_HEADER_BYTES = 30


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


def _extract_deflate64(zip_path: str, info: zipfile.ZipInfo, local_path: str) -> None:
    """Decompress one Deflate64 member with stream-inflate, a pure-Python inflater.

    zipfile still supplies the directory entry; only the member's compressed bytes
    are read here. The fast C extension for Deflate64 needs an x86 warehouse, and
    FAERS_WH runs on ARM. Pure Python manages ~6 MB/s, about seven minutes for a
    quarter, which only the few Deflate64 quarters pay.
    """
    with open(zip_path, "rb") as archive, open(local_path, "wb") as target:
        # The data starts after the member's local header, whose variable-length
        # name and extra fields can differ from the central directory's copy.
        archive.seek(info.header_offset)
        header = archive.read(LOCAL_HEADER_BYTES)
        name_length, extra_length = struct.unpack("<HH", header[26:30])
        archive.seek(info.header_offset + LOCAL_HEADER_BYTES + name_length + extra_length)

        def compressed_chunks():
            remaining = info.compress_size
            while remaining > 0:
                chunk = archive.read(min(DOWNLOAD_CHUNK_BYTES, remaining))
                if not chunk:
                    raise EOFError(f"{info.filename} is truncated")
                remaining -= len(chunk)
                yield chunk

        # stream-inflate does not check the CRC that zipfile would, so it is
        # checked here to keep a corrupt archive from being staged as valid XML.
        crc = 0
        uncompressed_chunks = stream_inflate64()[0]
        for chunk in uncompressed_chunks(compressed_chunks()):
            crc = zlib.crc32(chunk, crc)
            target.write(chunk)

    if crc != info.CRC or os.path.getsize(local_path) != info.file_size:
        raise ValueError(f"{info.filename} failed its CRC or size check after decompression")


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

            if info.compress_type == DEFLATE64:
                _extract_deflate64(zip_path, info, local_path)
            else:
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
