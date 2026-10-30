#!/bin/bash
set -euo pipefail

echo "============================================="
echo " Ожидание готовности HDFS"
echo "============================================="
MAX_ATTEMPTS=75
SLEEP_SECONDS=2
attempt=1
until hdfs dfs -ls / >/dev/null 2>&1; do
    if [ "${attempt}" -ge "${MAX_ATTEMPTS}" ]; then
        echo "HDFS недоступен после $((MAX_ATTEMPTS * SLEEP_SECONDS))с, прерываю." >&2
        exit 1
    fi
    echo "HDFS ещё недоступен (попытка ${attempt}/${MAX_ATTEMPTS}), жду ${SLEEP_SECONDS}с..."
    attempt=$((attempt + 1))
    sleep "${SLEEP_SECONDS}"
done
echo "HDFS доступен."

echo "============================================="
echo " Ожидание появления датасета /ml-latest-small"
echo "============================================="
DATA_MAX_ATTEMPTS=90
DATA_SLEEP_SECONDS=2
attempt=1
until hdfs dfs -test -e /ml-latest-small/ratings.csv >/dev/null 2>&1 \
      && hdfs dfs -test -e /ml-latest-small/tags.csv >/dev/null 2>&1; do
    if [ "${attempt}" -ge "${DATA_MAX_ATTEMPTS}" ]; then
        echo "Датасет /ml-latest-small (ratings.csv/tags.csv) не появился на HDFS." >&2
        echo "Проверьте фактический путь к датасету и поправьте ML_DIR в spark_app.py" >&2
        exit 1
    fi
    echo "Датасет ещё не готов (попытка ${attempt}/${DATA_MAX_ATTEMPTS}), жду ${DATA_SLEEP_SECONDS}с..."
    attempt=$((attempt + 1))
    sleep "${DATA_SLEEP_SECONDS}"
done
echo "Датасет найден."

echo "============================================="
echo " Запуск Spark-приложения через spark-submit"
echo "============================================="
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --conf spark.dynamicAllocation.enabled=false \
    /opt/app/spark_app.py

echo "============================================="
echo " Готово"
echo "============================================="
