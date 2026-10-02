from snowflake import snowpark_connect

from faers_ingestion.config import build_connection_parameters


def create_spark():
    """Start a Spark session backed by Snowflake warehouse compute.

    There is no cluster and no local JVM executor: Snowpark Connect translates the
    DataFrame plan and runs it in Snowflake, so no connector jars are needed.
    """
    snowpark_connect.start_session(connection_parameters=build_connection_parameters())

    return snowpark_connect.get_session()
