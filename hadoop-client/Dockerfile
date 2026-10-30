FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV JAVA_HOME=/opt/java/openjdk

# Установка Hadoop 3.3.6
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
    && rm -f /tmp/hadoop.tar.gz \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Конфигурация клиента: HDFS -> master:8020, YARN RM -> master:8032
COPY config/core-site.xml   ${HADOOP_CONF_DIR}/core-site.xml
COPY config/hdfs-site.xml   ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY config/yarn-site.xml   ${HADOOP_CONF_DIR}/yarn-site.xml
COPY config/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /root

ENTRYPOINT ["/entrypoint.sh"]
