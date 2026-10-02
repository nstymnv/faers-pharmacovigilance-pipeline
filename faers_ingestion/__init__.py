import importlib.util
import os
import sys
from pathlib import Path

# PySpark 3.5 ships separate classic and Spark Connect implementations of
# pyspark.sql.functions, and the classic one asserts on an active SparkContext —
# which does not exist when execution happens on a Snowflake warehouse. This flag
# makes the plain `pyspark.sql.functions` imports resolve to the Connect versions,
# so the DataFrame code needs no Connect-specific imports.
os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")


def _expose_bundled_pyspark() -> None:
    """Put the pyspark that ships inside snowpark-connect on the import path.

    snowpark-connect vendors pyspark rather than depending on it, and only adds it
    to sys.path once a session starts. Without this, importing any module that
    annotates a DataFrame fails before a session can be created.
    """
    spec = importlib.util.find_spec("snowflake.snowpark_connect")
    if spec is None or spec.origin is None:
        return

    includes = Path(spec.origin).parent / "includes" / "python"
    if includes.is_dir() and str(includes) not in sys.path:
        sys.path.insert(0, str(includes))


_expose_bundled_pyspark()
