# big-data-labs — лабораторные по больших данных (Hadoop HDFS/MapReduce + Apache Spark)

Две независимые лабораторные работы, каждая — Docker-образ «клиента», подключаемогося к
учебному кластеру Hadoop (HDFS + YARN):

1. **hadoop-client** — операции с HDFS и запуск MapReduce-джобы wordcount через YARN.
2. **spark-homework** — Spark-приложение на PySpark: агрегаты по датасету
   `ml-latest-small`, обучение TF-IDF + SGDRegressor, предсказание рейтинга через UDF
   и broadcast-переменные, запись результата на HDFS.

Образы собираются локально и запускаются **против уже работающего кластера**:
в конфигах указан адрес учебного кластера `192.168.34.2` (HDFS `:8020`,
YARN Resource Manager `:8032`). При запуске на своём кустере замените IP во всех
`config/*.xml` на адрес своего NameNode/Resource Manager.

---

## Лабораторная 1: hadoop-client (HDFS + MapReduce wordcount)

**Каталог:** [`hadoop-client/`](hadoop-client/)

**Что делает** ([`entrypoint.sh`](hadoop-client/entrypoint.sh), все шаги нумерованы в логе):

1. Ждёт готовности HDFS (до 75 попыток по 2 с) — кластер запускается отдельно.
2. Создаёт директорию `/createme` на HDFS.
3. Удаляет директорию `/delme` (если была).
4. Создаёт файл `/nonnull.txt` с содержимым (дата генерации в UTC).
5. Ждёт появления заданного файла `/shadow.txt` на HDFS и запускает на нём
   **MapReduce wordcount** штатным примером из дистрибутива Hadoop:
   `hadoop jar hadoop-mapreduce-examples-3.3.6.jar wordcount /shadow.txt /wordcount_output`.
6. Суммирует вхождения слова `Innsmouth` из `part-r-*` и записывает число
   в `/whataboutinsmouth.txt`.

**Стек:** образ на `eclipse-temurin:8-jdk-jammy` + Hadoop **3.3.6** (скачивается с
archive.apache.org при сборке), конфиги клиента — [`hadoop-client/config/`](hadoop-client/config/).

**Запуск** (Docker и доступ к кластеру обязательны):

```bash
cd hadoop-client
docker build -t arkanoid-hadoop-client .
docker run --rm arkanoid-hadoop-client
```

Успешный прогон заканчивается блоком «Все задачи успешно выполнены» и выводом
числа вхождений слова. Ошибки окружения (нет Docker/кластера) здесь не
воспроизводятся — работа проверялась только на целевом кластере.

---

## Лабораторная 2: spark-homework (PySpark-приложение)

**Каталог:** [`spark-homework/`](spark-homework/), приложение —
[`app/spark_app.py`](spark-homework/app/spark_app.py) (271 строка, Python 3, PySpark 3.5.1).

**Что делает** (шаги выводятся в лог нумерованно):

1. Поднимает `SparkSession` с `master=yarn`, 2 executor'ами
   (`--num-executors 2`, `spark-submit --deploy-mode client`).
2. Создаёт (перезаписывает) пустой файл `/sparkExperiments.txt` на HDFS.
3. Читает `ratings.csv` и `tags.csv` датасета `ml-latest-small` с HDFS,
   считает строки, а также число стейджей и тасок, потраченных на чтение.
4. Считает число уникальных фильмов и пользователей в `ratings`.
5. Считает число оценок `>= 4.0`.
6. Считает среднюю разницу во времени между тегом и оценкой (в секундах).
7. Считает среднюю оценку каждого пользователя, затем среднее от этих средних.
8. Обучает **TF-IDF (scikit-learn) + SGDRegressor** на парах `(tag → rating)`,
   экспортирует параметры модели в broadcast-переменные и предсказывает рейтинг
   уже на кластере через **пользовательскую UDF** (executor'ам не нужен
   scikit-learn — только словарь, idf и коэффициенты), считает RMSE.

Все результаты собираются в памяти и одним `overwrite` записываются в
`/sparkExperiments.txt` — намеренно без `append`, так как это надёжнее на
однонодовых/тестовых кластерах.

**Стек:** тот же базовый образ + Hadoop CLI 3.3.6, `pyspark==3.5.1`,
`scikit-learn`, `pandas`, `numpy` (см. [`spark-homework/Dockerfile`](spark-homework/Dockerfile)).

**Запуск:**

```bash
cd spark-homework
docker build -t arkanoid-spark-homework .
docker run --rm arkanoid-spark-homework
```

`entrypoint.sh` ждёт готовности HDFS и появления датасета
`/ml-latest-small/ratings.csv` + `tags.csv`, затем запускает
`spark-submit --master yarn --deploy-mode client /opt/app/spark_app.py`.
При другом расположении датасета поправьте `ML_DIR` в начале `spark_app.py`.

---

## Структура репозитория

```
hadoop-client/
├── Dockerfile          # Hadoop 3.3.6 + конфиги клиента
├── entrypoint.sh       # шаги лабораторной: HDFS-операции + wordcount через YARN
└── config/             # core-site, hdfs-site, yarn-site, mapred-site

spark-homework/
├── Dockerfile          # Hadoop CLI + PySpark 3.5.1 + sklearn
├── entrypoint.sh       # ожидание HDFS/датасета + spark-submit
├── app/spark_app.py    # само приложение (8 шагов, TF-IDF + SGDRegressor)
└── config/             # core-site, hdfs-site, yarn-site, mapred-site
```

## Что проверено без кластера

- `bash -n` — оба `entrypoint.sh` проходят синтаксическую проверку.
- `python3 -m py_compile` — `spark_app.py` компилируется без ошибок.
- Сборка образов и запуск против кластера **не выполнялись** в этой среде
  (Docker недоступен) — работа проверялась на учебном кластере, адрес которого
  прописан в конфигах (`192.168.34.2`).
