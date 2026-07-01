FROM python:3.11-bullseye as spark-base

ARG SPARK_VERSION=3.5.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        sudo \
        curl \
        vim \
        unzip \
        rsync \
        openjdk-11-jdk \
        build-essential \
        software-properties-common \
        ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN set -eux; \
    curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz --directory /opt/spark --strip-components 1; \
    rm -f /tmp/spark.tgz

COPY requirements/requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

WORKDIR /
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh \
 && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]