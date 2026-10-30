#!/bin/bash
set -euo pipefail

echo "============================================="
echo " 0) Ожидание готовности HDFS/YARN"
echo "============================================="
MAX_ATTEMPTS=75
SLEEP_SECONDS=2
attempt=1
until hdfs dfs -ls / >/dev/null 2>&1; do
    if [ "${attempt}" -ge "${MAX_ATTEMPTS}" ]; then
        echo "HDFS недоступен после ${MAX_ATTEMPTS} попыток, прерываю." >&2
        exit 1
    fi
    echo "HDFS ещё недоступен (попытка ${attempt}/${MAX_ATTEMPTS}), жду ${SLEEP_SECONDS}с..."
    attempt=$((attempt + 1))
    sleep "${SLEEP_SECONDS}"
done
echo "HDFS доступен, продолжаю."

echo "============================================="
echo " 1) Создание директории /createme на HDFS"
echo "============================================="
hdfs dfs -mkdir -p /createme
hdfs dfs -ls / | grep createme

echo "============================================="
echo " 2) Удаление директории /delme на HDFS"
echo "============================================="
hdfs dfs -rm -r -f -skipTrash /delme || echo "Директория /delme отсутствовала (уже удалена)"

echo "============================================="
echo " 3) Создание файла /nonnull.txt с содержимым"
echo "============================================="
echo "Hadoop test content, generated at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | hdfs dfs -put -f - /nonnull.txt
hdfs dfs -cat /nonnull.txt

echo "============================================="
echo " 4) MapReduce wordcount для /shadow.txt через YARN"
echo "============================================="
SHADOW_MAX_ATTEMPTS=90
SHADOW_SLEEP_SECONDS=2
attempt=1
until hdfs dfs -test -e /shadow.txt >/dev/null 2>&1; do
    if [ "${attempt}" -ge "${SHADOW_MAX_ATTEMPTS}" ]; then
        echo "Файл /shadow.txt так и не появился на HDFS после $((SHADOW_MAX_ATTEMPTS * SHADOW_SLEEP_SECONDS)) секунд ожидания." >&2
        exit 1
    fi
    echo "/shadow.txt ещё не появился на HDFS (попытка ${attempt}/${SHADOW_MAX_ATTEMPTS}), жду ${SHADOW_SLEEP_SECONDS}с..."
    attempt=$((attempt + 1))
    sleep "${SHADOW_SLEEP_SECONDS}"
done
echo "/shadow.txt найден на HDFS, продолжаю."

hdfs dfs -rm -r -f -skipTrash /wordcount_output || true

hadoop jar "${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar" \
    wordcount /shadow.txt /wordcount_output

echo "Результат джобы записан в /wordcount_output"

echo "============================================="
echo " 5) Подсчёт вхождений слова 'Innsmouth'"
echo "============================================="
COUNT=$(hdfs dfs -cat /wordcount_output/part-r-* 2>/dev/null \
    | grep -P '^Innsmouth\t' \
    | awk -F'\t' '{sum += $2} END {print sum+0}')

echo "Слово 'Innsmouth' встречается ${COUNT} раз(а)"
printf '%s' "${COUNT}" | hdfs dfs -put -f - /whataboutinsmouth.txt
hdfs dfs -cat /whataboutinsmouth.txt

echo "============================================="
echo " Все задачи успешно выполнены"
echo "============================================="
