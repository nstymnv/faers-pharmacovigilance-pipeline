"""The stored procedure's ZIP handling, run locally against a fake session.

Only the member selection and staging logic is tested here; the download and the
INGESTION_LOG writes need Snowflake. Deflate64 members are not covered: Python's
zipfile cannot write them, so there is no way to build a fixture here.
"""

import os
import zipfile

from faers_ingestion.sprocs.ingest_faers_quarter import _stage_members


class FakeFileOperations:
    def __init__(self):
        self.puts = []

    def put(self, local_uri, stage_prefix, auto_compress, overwrite):
        local_path = local_uri.removeprefix("file://")
        with open(local_path, "rb") as handle:
            content = handle.read()
        self.puts.append(
            {
                "local_path": local_path,
                "name": os.path.basename(local_path),
                "stage_prefix": stage_prefix,
                "content": content,
                "auto_compress": auto_compress,
            }
        )


class FakeSession:
    def __init__(self):
        self.file = FakeFileOperations()


def build_zip(path, members):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in members.items():
            archive.writestr(name, content)


def test_stages_xml_and_deleted_files_only(tmp_path):
    zip_path = tmp_path / "faers.zip"
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    build_zip(
        zip_path,
        {
            "XML/1_ADR21Q1.xml": "<ichicsr>one</ichicsr>",
            "XML/2_ADR21Q1.XML": "<ichicsr>two</ichicsr>",
            "Deleted/ADR21Q1DeletedCases.txt": "12345\n",
            "FAQs.pdf": "not staged",
            "Readme.txt": "not staged either",
            "XML/": "",
        },
    )
    session = FakeSession()

    files, staged_bytes = _stage_members(session, str(zip_path), str(work_dir), 2021, 1)

    staged = {put["name"]: put for put in session.file.puts}
    assert files == 3
    assert set(staged) == {"1_ADR21Q1.xml", "2_ADR21Q1.XML", "ADR21Q1DeletedCases.txt"}
    assert (
        staged["1_ADR21Q1.xml"]["stage_prefix"] == "@extraction.faers_raw/xml/year=2021/quarter=1"
    )
    assert staged["ADR21Q1DeletedCases.txt"]["stage_prefix"] == (
        "@extraction.faers_raw/deleted/year=2021/quarter=1"
    )
    assert staged["1_ADR21Q1.xml"]["content"] == b"<ichicsr>one</ichicsr>"
    assert staged_bytes == sum(len(put["content"]) for put in session.file.puts)


def test_xml_is_staged_uncompressed(tmp_path):
    # Snowpark Connect cannot read compressed XML from a stage.
    zip_path = tmp_path / "faers.zip"
    build_zip(zip_path, {"XML/1_ADR21Q1.xml": "<ichicsr/>"})
    session = FakeSession()

    _stage_members(session, str(zip_path), str(tmp_path), 2021, 1)

    assert session.file.puts[0]["auto_compress"] is False


def test_member_paths_cannot_escape_the_work_dir(tmp_path):
    zip_path = tmp_path / "faers.zip"
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    build_zip(zip_path, {"../../escaped.xml": "<ichicsr/>", "/abs/also.xml": "<ichicsr/>"})
    session = FakeSession()

    _stage_members(session, str(zip_path), str(work_dir), 2021, 1)

    for put in session.file.puts:
        assert os.path.dirname(put["local_path"]) == str(work_dir)
    assert not (tmp_path / "escaped.xml").exists()


def test_local_copies_are_removed_after_staging(tmp_path):
    # The warehouse's scratch disk holds one member at a time, not a quarter.
    zip_path = tmp_path / "faers.zip"
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    build_zip(zip_path, {"XML/1.xml": "<a/>", "XML/2.xml": "<b/>"})

    _stage_members(FakeSession(), str(zip_path), str(work_dir), 2021, 1)

    assert list(work_dir.iterdir()) == []
