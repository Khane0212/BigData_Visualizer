# data_loader.py
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, regexp_replace 

@st.cache_resource
def init_spark():
    """Khởi tạo Spark Session với MongoDB Connector"""
    try:
        mongo_uri = st.secrets["mongo"]["uri"]
        spark = SparkSession.builder \
            .appName("WikipediaBigData") \
            .config("spark.mongodb.read.connection.uri", mongo_uri) \
            .config("spark.mongodb.write.connection.uri", mongo_uri) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        return spark
    except Exception as e:
        st.error(f"Lỗi khởi động Spark: {e}")
        return None

@st.cache_data(ttl=600)
def load_and_process_data(db_name, col_name):
    """
    Load data từ Mongo qua Spark, xử lý aggregation
    """
    spark = init_spark()
    if not spark: return None

    try:
        mongo_uri = st.secrets["mongo"]["uri"]
    except:
        st.error("Không tìm thấy URI trong secrets.toml")
        return None

    df_spark = spark.read.format("mongodb") \
        .option("connection.uri", mongo_uri) \
        .option("database", db_name) \
        .option("collection", col_name) \
        .load()
    
    if "word_count" in df_spark.columns:
        df_spark = df_spark.withColumn("word_count", col("word_count").cast("string"))
        df_spark = df_spark.withColumn("word_count", regexp_replace(col("word_count"), ",", ""))
        df_spark = df_spark.withColumn("word_count", col("word_count").cast("int"))
        df_spark = df_spark.fillna(0, subset=["word_count"])

    if "text_len" in df_spark.columns:
        df_spark = df_spark.withColumn("text_len", col("text_len").cast("string"))
        df_spark = df_spark.withColumn("text_len", regexp_replace(col("text_len"), ",", ""))
        df_spark = df_spark.withColumn("text_len", col("text_len").cast("int"))

    if "rev_ts" in df_spark.columns:
        df_spark = df_spark.withColumn("timestamp", to_timestamp(col("rev_ts")))
        df_spark = df_spark.withColumn("year", year(col("timestamp")))
    
    df_spark.cache()


    pdf_year = df_spark.groupBy("year").count().orderBy("year").toPandas()

    pdf_top10 = df_spark.select("title", "word_count") \
        .orderBy(col("word_count").desc()) \
        .limit(10).toPandas()

    pdf_dist = df_spark.select("word_count").toPandas()

    if "text_clean" in df_spark.columns:
        pdf_text = df_spark.select("text_clean").sample(False, 0.1).limit(100).toPandas()
    else:
        pdf_text = pd.DataFrame()

    total_count = df_spark.count()
    max_word_count = pdf_top10['word_count'].max() if not pdf_top10.empty else 0
    max_year = pdf_year["year"].max() if not pdf_year.empty else 0

    df_raw_safe = df_spark.withColumn("timestamp", col("timestamp").cast("string"))
    
    if "rev_ts" in df_spark.columns:
        df_raw_safe = df_raw_safe.withColumn("rev_ts", col("rev_ts").cast("string"))

    pdf_raw = df_raw_safe.toPandas()

    return {
        "stats_year": pdf_year,
        "top_10": pdf_top10,
        "distribution": pdf_dist,
        "sample_text": pdf_text,
        "kpi": {
            "total_docs": total_count,
            "max_len": max_word_count,
            "latest_year": max_year
        },
        "raw_data": pdf_raw  
    }