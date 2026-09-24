"""Quarterly FAERS pipeline: stage → load → dbt, one quarter at a time.

    discover_quarters                pending quarters from INGESTION_LOG, capped
      └─ quarter (mapped per quarter)
           ├─ ingest_quarter         CALL extraction.ingest_faers_quarter (in Snowflake)
           └─ extract_load           python -m faers_ingestion.main (Snowpark Connect)
                └─ loads_done
                     └─ dbt          Cosmos: one task per model, tests after each

Airflow only issues commands. The download, the XML parsing and every dbt model
run on Snowflake compute, so nothing is stored on the machine running Airflow.

Backfill runs through this DAG too, max_quarters at a time. The default of 1
keeps an accidental trigger to one quarter's credits; raise it for the backfill
once the per-quarter cost is known.
"""

import logging
import os
import re
import subprocess
from datetime import date

import pendulum
from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Param, TriggerRule, dag, task, task_group
from airflow.sdk.exceptions import AirflowSkipException
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.profiles import SnowflakePrivateKeyFilePemProfileMapping

SNOWFLAKE_CONN_ID = "faers_snowflake"

PROJECT_DIR = "/opt/airflow/project"
INGESTION_PYTHON = "/opt/airflow/venvs/ingestion/bin/python"
DBT_EXECUTABLE = "/opt/airflow/venvs/dbt/bin/dbt"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt/faers_transformations"

# Same value as dbt's mart_start_quarter var: quarters before the analysis window
# are never loaded automatically. 2020q1, the dev slice, can still be loaded by
# naming it in the quarters param.
FIRST_QUARTER = "2021q1"

# Written by faers_ingestion.main (config.LOAD_SUCCEEDED_STATUS). Duplicated
# rather than imported because the ingestion package lives in its own
# virtualenv, not in Airflow's.
LOADED_STATUS = "LOADED"

# The procedure raises this when FDA has not published the quarter yet. That is
# the expected state of the newest quarter for a few weeks after it ends, so it
# skips the quarter instead of failing the run.
NOT_PUBLISHED_MARKER = "has not published"

QUARTER_PATTERN = re.compile(r"^(20\d{2})q([1-4])$")

logger = logging.getLogger(__name__)


def parse_quarter(quarter_key: str) -> tuple[int, int]:
    match = QUARTER_PATTERN.match(quarter_key)
    if not match:
        raise ValueError(f"expected a quarter like 2021q1, got {quarter_key!r}")

    return int(match.group(1)), int(match.group(2))


def completed_quarters(first: str, today: date) -> list[str]:
    """Every quarter from `first` up to the last one that has fully ended."""
    year, quarter = parse_quarter(first)
    current = (today.year, (today.month - 1) // 3 + 1)

    quarters = []
    while (year, quarter) < current:
        quarters.append(f"{year}q{quarter}")
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)

    return quarters


def snowflake_environment() -> dict[str, str]:
    """The SNOWFLAKE_* variables faers_ingestion.config reads, from the Airflow connection.

    The connection is the single place credentials live; the ingestion code keeps
    reading environment variables so it runs the same from a shell and from here.
    """
    conn = SnowflakeHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson

    return {
        "SNOWFLAKE_ACCOUNT": extra["account"],
        "SNOWFLAKE_USER": conn.login,
        "SNOWFLAKE_PRIVATE_KEY_PATH": extra["private_key_file"],
        "SNOWFLAKE_WAREHOUSE": extra["warehouse"],
        "SNOWFLAKE_DATABASE": extra["database"],
        "SNOWFLAKE_SCHEMA": conn.schema,
        "SNOWFLAKE_ROLE": extra["role"],
    }


@dag(
    dag_id="faers_quarterly",
    # FDA publishes a quarter some weeks after it ends; the 15th of the last month
    # of the following quarter leaves room for that. A quarter still unpublished
    # is skipped and picked up by the next run. Airflow only runs while the laptop
    # is on, so in practice most runs are triggered by hand.
    schedule="0 6 15 3,6,9,12 *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # So the dbt_vars param reaches Cosmos as a dict rather than its string form.
    render_template_as_native_obj=True,
    default_args={"retries": 0},
    params={
        "quarters": Param(
            [],
            type="array",
            items={"type": "string", "pattern": QUARTER_PATTERN.pattern},
            description=(
                "Load exactly these quarters (e.g. 2020q1), skipping discovery "
                "and max_quarters. Empty: load pending quarters from "
                f"{FIRST_QUARTER} on."
            ),
        ),
        "max_quarters": Param(
            1,
            type="integer",
            minimum=1,
            description="How many pending quarters to load in this run, oldest first.",
        ),
        "force_stage": Param(
            False,
            type="boolean",
            description="Re-download and re-stage quarters already staged.",
        ),
        "dbt_vars": Param(
            {},
            type="object",
            description=(
                'dbt vars for this run, e.g. {"faers_quarters": ["2020q1"]} to '
                "build on the dev slice. Empty: the full warehouse, with the "
                "marts restricted to the analysis window."
            ),
        ),
    },
    tags=["faers"],
)
def faers_quarterly():
    @task
    def discover_quarters(params: dict) -> list[str]:
        if params["quarters"]:
            logger.info("loading the requested quarters: %s", params["quarters"])
            return params["quarters"]

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        loaded = {
            row[0]
            for row in hook.get_records(
                "select distinct quarter_key from extraction.ingestion_log where status = %s",
                parameters=(LOADED_STATUS,),
            )
        }

        pending = [
            q
            for q in completed_quarters(FIRST_QUARTER, pendulum.today("UTC").date())
            if q not in loaded
        ]
        selected = pending[: params["max_quarters"]]
        logger.info("%d quarters pending; this run loads %s", len(pending), selected)

        return selected

    @task_group
    def quarter(quarter_key: str):
        @task(retries=1, retry_delay=pendulum.duration(minutes=5), max_active_tis_per_dagrun=1)
        def ingest_quarter(quarter_key: str, params: dict) -> None:
            year, quarter = parse_quarter(quarter_key)
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

            try:
                # autocommit=True, which get_first() would not set: with it off,
                # the CALL runs inside an open transaction, and Snowflake rolls
                # back the procedure's INGESTION_LOG insert and fails the call
                # ("Scoped transaction started in stored procedure is incomplete").
                result = hook.run(
                    "call extraction.ingest_faers_quarter(%s, %s, %s)",
                    parameters=(year, quarter, params["force_stage"]),
                    autocommit=True,
                    handler=fetch_one_handler,
                )
            except Exception as exc:
                if NOT_PUBLISHED_MARKER in str(exc):
                    raise AirflowSkipException(f"FDA has not published {quarter_key} yet") from exc
                raise

            logger.info("staged %s: %s", quarter_key, result[0])

        # One quarter at a time: each load parses ~2 GB of XML on the warehouse,
        # and running several at once only queues them on an XSMALL while holding
        # a JVM per load in this container's memory.
        @task(max_active_tis_per_dagrun=1)
        def extract_load(quarter_key: str) -> None:
            # A subprocess rather than an import: Snowpark Connect starts a JVM in
            # the calling process (one per process, never restarted) and its pins
            # conflict with Airflow's, so it lives in its own virtualenv.
            env = {
                **os.environ,
                **snowflake_environment(),
                "PYTHONPATH": f"{PROJECT_DIR}/src",
            }
            command = [INGESTION_PYTHON, "-m", "faers_ingestion.main", "--quarters", quarter_key]

            with subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            ) as process:
                for line in process.stdout:
                    logger.info(line.rstrip())

            if process.returncode != 0:
                raise RuntimeError(f"loading {quarter_key} exited with {process.returncode}")

        ingest_quarter(quarter_key) >> extract_load(quarter_key)

    # Builds once, after every quarter in the run. It still runs when some quarters
    # were skipped as unpublished, but not after a failed load, and not when no
    # quarter loaded at all.
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def loads_done() -> None:
        pass

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="faers_transformations",
            target_name="prod",
            profile_mapping=SnowflakePrivateKeyFilePemProfileMapping(conn_id=SNOWFLAKE_CONN_ID),
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            # A test that reads several models (relationships tests, the
            # int-to-fact row-count tests) would otherwise run in the test task
            # of whichever parent finishes first, against the other parents'
            # stale tables from the previous run. Detached, it gets its own task
            # after all of its parents are built.
            should_detach_multiple_parents_tests=True,
            # Packages are installed once by the compose file's dbt-deps service,
            # not before every one of the ~30 tasks.
            dbt_deps=False,
        ),
        operator_args={
            "install_deps": False,
            "vars": "{{ params.dbt_vars }}",
        },
        default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    )

    quarter.expand(quarter_key=discover_quarters()) >> loads_done() >> dbt


faers_quarterly()
