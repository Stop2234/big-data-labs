#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark-приложение для домашнего задания "Работа со Spark".

Выполняет по порядку:
1. Поднимает SparkSession с master=yarn и 2 executor'ами.
2. Создаёт пустой файл /sparkExperiments.txt на HDFS.
3. Читает ratings и tags из ml-latest-small, считает строки,
   а также число стейджей и тасок, потраченных на это чтение.
4. Считает уникальные фильмы и юзеров в ratings.
5. Считает число оценок >= 4.0.
6. Считает среднюю разницу во времени между тегом и оценкой (в секундах).
7. Считает среднюю оценку по каждому юзеру, затем среднее от этих средних.
8. Обучает TF-IDF + SGDRegressor на (tag -> rating), делает UDF-предсказание,
   считает RMSE (буквально: корень суммы квадратов разностей).

Итоговое содержимое файла собирается в памяти и одним финальным
overwrite записывается в /sparkExperiments.txt (не полагаемся на append
к HDFS-файлам, т.к. это менее надёжно на однонодовых/тестовых кластерах).
"""

import re
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDRegressor

# ------------------------------------------------------------------
# ПУТИ. Если на сервере датасет лежит по другому пути — поменяйте здесь.
# ------------------------------------------------------------------
ML_DIR = "/ml-latest-small"
RATINGS_PATH = f"{ML_DIR}/ratings.csv"
TAGS_PATH = f"{ML_DIR}/tags.csv"
OUTPUT_PATH = "/sparkExperiments.txt"


def write_hdfs_file(spark, path, content):
    """Создать/перезаписать один обычный файл на HDFS через Hadoop FileSystem API."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hpath):
        fs.delete(hpath, False)
    out = fs.create(hpath, True)
    try:
        if content:
            out.write(bytearray(content.encode("utf-8")))
    finally:
        out.close()


def main():
    spark = (
        SparkSession.builder.appName("SparkHomework")
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.executor.instances", "2")
        .config("spark.dynamicAllocation.enabled", "false")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("=" * 60)
    print("1) SparkSession запущена: master=%s, appId=%s" % (sc.master, sc.applicationId))
    print("=" * 60)

    print("=" * 60)
    print("2) Создание пустого файла /sparkExperiments.txt")
    print("=" * 60)
    write_hdfs_file(spark, OUTPUT_PATH, "")

    result_lines = []

    # ------------------------------------------------------------
    # 3) Чтение ratings/tags + подсчёт стейджей и тасок
    # ------------------------------------------------------------
    print("=" * 60)
    print("3) Чтение ratings/tags, подсчёт строк / стейджей / тасок")
    print("=" * 60)

    sc.setJobGroup("countRowsGroup", "Count ratings and tags rows")

    ratings_df = (
        spark.read.option("header", True).option("inferSchema", True).csv(RATINGS_PATH)
    )
    tags_df = (
        spark.read.option("header", True).option("inferSchema", True).csv(TAGS_PATH)
    )

    ratings_count = ratings_df.count()
    tags_count = tags_df.count()

    sc.setJobGroup("", "")

    tracker = sc.statusTracker()
    job_ids = tracker.getJobIdsForGroup("countRowsGroup")

    seen_stages = set()
    total_tasks = 0
    for jid in job_ids:
        job_info = tracker.getJobInfo(jid)
        if job_info is None:
            continue
        for sid in job_info.stageIds:
            if sid in seen_stages:
                continue
            seen_stages.add(sid)
            stage_info = tracker.getStageInfo(sid)
            if stage_info is not None:
                total_tasks += stage_info.numTasks

    total_stages = len(seen_stages)

    print(f"ratings rows = {ratings_count}, tags rows = {tags_count}")
    print(f"stages = {total_stages}, tasks = {total_tasks}")
    result_lines.append(f"stages:{total_stages} tasks:{total_tasks}")

    ratings_df = ratings_df.cache()
    tags_df = tags_df.cache()

    # ------------------------------------------------------------
    # 4) Уникальные фильмы и юзеры
    # ------------------------------------------------------------
    print("=" * 60)
    print("4) Уникальные фильмы и юзеры в ratings")
    print("=" * 60)
    films_unique = ratings_df.select("movieId").distinct().count()
    users_unique = ratings_df.select("userId").distinct().count()
    print(f"filmsUnique = {films_unique}, usersUnique = {users_unique}")
    result_lines.append(f"filmsUnique:{films_unique} usersUnique:{users_unique}")

    # ------------------------------------------------------------
    # 5) Число оценок >= 4.0
    # ------------------------------------------------------------
    print("=" * 60)
    print("5) Число оценок >= 4.0")
    print("=" * 60)
    good_rating_count = ratings_df.filter(F.col("rating") >= 4.0).count()
    print(f"goodRating = {good_rating_count}")
    result_lines.append(f"goodRating:{good_rating_count}")

    # ------------------------------------------------------------
    # 6) Средняя разница во времени между тегом и оценкой
    # ------------------------------------------------------------
    print("=" * 60)
    print("6) Средняя разница во времени (тег - оценка), секунды")
    print("=" * 60)
    joined_tr = tags_df.alias("t").join(
        ratings_df.alias("r"), on=["userId", "movieId"], how="inner"
    )
    diff_df = joined_tr.withColumn(
        "timeDiff",
        F.col("t.timestamp").cast("long") - F.col("r.timestamp").cast("long"),
    )
    avg_time_diff_row = diff_df.select(F.avg("timeDiff").alias("avgDiff")).collect()[0]
    avg_time_diff = avg_time_diff_row["avgDiff"]
    if avg_time_diff is None:
        avg_time_diff = 0.0
    print(f"timeDifference = {avg_time_diff}")
    result_lines.append(f"timeDifference:{avg_time_diff}")

    # ------------------------------------------------------------
    # 7) Средняя оценка по юзеру -> среднее от этих средних
    # ------------------------------------------------------------
    print("=" * 60)
    print("7) Средняя оценка по каждому юзеру, усреднённая по всем юзерам")
    print("=" * 60)
    per_user_avg = ratings_df.groupBy("userId").agg(F.avg("rating").alias("userAvg"))
    avg_of_avgs_row = per_user_avg.select(F.avg("userAvg").alias("avgOfAvg")).collect()[0]
    avg_of_avgs = avg_of_avgs_row["avgOfAvg"]
    if avg_of_avgs is None:
        avg_of_avgs = 0.0
    print(f"avgRating = {avg_of_avgs}")
    result_lines.append(f"avgRating:{avg_of_avgs}")

    # ------------------------------------------------------------
    # 8) TF-IDF + SGDRegressor + UDF + RMSE
    # ------------------------------------------------------------
    print("=" * 60)
    print("8) Обучение TF-IDF + SGDRegressor, применение UDF, RMSE")
    print("=" * 60)

    train_pdf = (
        joined_tr.select(F.col("t.tag").alias("tag"), F.col("r.rating").alias("rating"))
        .dropna()
        .toPandas()
    )

    tag_texts = train_pdf["tag"].astype(str).tolist()
    tag_ratings = train_pdf["rating"].astype(float).tolist()

    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(tag_texts)

    model = SGDRegressor(max_iter=1000, tol=1e-3, random_state=42)
    model.fit(X, tag_ratings)

    # Экспортируем параметры модели в обычные python-структуры, чтобы UDF
    # не зависела от scikit-learn на executor'ах (там его может не быть).
    vocabulary = dict(vectorizer.vocabulary_)  # word -> index
    idf_values = vectorizer.idf_.tolist()
    coef_values = model.coef_.tolist()
    intercept_value = float(model.intercept_[0])
    token_pattern = re.compile(r"(?u)\b\w\w+\b")

    b_vocab = sc.broadcast(vocabulary)
    b_idf = sc.broadcast(idf_values)
    b_coef = sc.broadcast(coef_values)
    b_intercept = sc.broadcast(intercept_value)

    def predict_rating(tag_text):
        if tag_text is None:
            return float(b_intercept.value)
        tokens = token_pattern.findall(str(tag_text).lower())
        vocab = b_vocab.value
        counts = {}
        for tok in tokens:
            if tok in vocab:
                counts[tok] = counts.get(tok, 0) + 1
        if not counts:
            return float(b_intercept.value)
        idf_local = b_idf.value
        raw = {tok: cnt * idf_local[vocab[tok]] for tok, cnt in counts.items()}
        norm = math.sqrt(sum(v * v for v in raw.values()))
        if norm == 0:
            return float(b_intercept.value)
        coef_local = b_coef.value
        dot = sum((val / norm) * coef_local[vocab[tok]] for tok, val in raw.items())
        return float(dot + b_intercept.value)

    predict_udf = F.udf(predict_rating, DoubleType())

    scored_df = joined_tr.select(
        F.col("t.tag").alias("tag"), F.col("r.rating").alias("rating")
    ).withColumn("predictedRating", predict_udf(F.col("tag")))

    # action, чтобы убедиться, что UDF реально отрабатывает
    scored_df.show(50, truncate=False)

    rmse_row = scored_df.select(
        F.sqrt(F.sum(F.pow(F.col("predictedRating") - F.col("rating"), 2.0))).alias("rmse")
    ).collect()[0]
    rmse_value = rmse_row["rmse"]
    if rmse_value is None:
        rmse_value = 0.0
    print(f"rmse = {rmse_value}")
    result_lines.append(f"rmse:{rmse_value}")

    # ------------------------------------------------------------
    # Финальная запись результата
    # ------------------------------------------------------------
    final_content = "\n".join(result_lines) + "\n"
    print("=" * 60)
    print("Итоговое содержимое /sparkExperiments.txt:")
    print(final_content)
    print("=" * 60)

    write_hdfs_file(spark, OUTPUT_PATH, final_content)

    spark.stop()


if __name__ == "__main__":
    main()
