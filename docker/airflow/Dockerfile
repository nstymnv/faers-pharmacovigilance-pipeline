# Airflow image for the FAERS pipeline. Built from the repo root (see
# docker-compose.yaml) so it can read the project's requirements files.
FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

# Cosmos only. Installed against Airflow's constraints so it cannot drag
# Airflow's own dependencies to versions Airflow was not released with.
COPY docker/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt in its own virtualenv, the layout Cosmos recommends: Cosmos calls the dbt
# executable, so dbt's dependency pins never meet Airflow's.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/venvs/dbt \
    && /opt/airflow/venvs/dbt/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# The ingestion runtime in its own virtualenv too. Snowpark Connect pins
# protobuf, grpcio, pandas and pyarrow ranges of its own, and starts a JVM in
# the calling process, so it runs as a subprocess rather than inside a task.
# The [jdk] extra bundles a JDK 17 (jdk4py), which Snowpark Connect falls back
# to when JAVA_HOME is unset — no system Java package needed.
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow/venvs/ingestion \
    && /opt/airflow/venvs/ingestion/bin/pip install --no-cache-dir \
        -r /tmp/requirements.txt \
        "snowpark-connect[jdk]"
