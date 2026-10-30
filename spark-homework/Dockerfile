FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV SPARK_VERSION=${SPARK_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PYSPARK_PYTHON=python3
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

# --- Hadoop 3.3.6 (CLI + конфиги для доступа к удалённому кластеру) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl python3 python3-pip \
    && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
    && rm -f /tmp/hadoop.tar.gz \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# --- Python / PySpark / sklearn стек ---
RUN pip3 install --no-cache-dir \
        pyspark==${SPARK_VERSION} \
        scikit-learn \
        pandas \
        numpy \
    && SPARK_DIR=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))") \
    && ln -s "${SPARK_DIR}" /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin

# --- Конфигурация клиента: HDFS -> 192.168.34.2:8020, YARN RM -> 192.168.34.2:8032 ---
COPY config/core-site.xml   ${HADOOP_CONF_DIR}/core-site.xml
COPY config/hdfs-site.xml   ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY config/yarn-site.xml   ${HADOOP_CONF_DIR}/yarn-site.xml
COPY config/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml

# --- Приложение ---
COPY app/spark_app.py /opt/app/spark_app.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /root

ENTRYPOINT ["/entrypoint.sh"]
