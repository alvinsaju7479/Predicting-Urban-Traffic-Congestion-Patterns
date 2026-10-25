import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import joblib
import pandas as pd
BASE_PATH = "/Users/alvinsaju/Desktop/DISSERTATION/Predicting Urban Traffic Congestion Patterns"
# Load model and scaler
model = joblib.load(f"{BASE_PATH}/MACHINE_LEARNING/model.pkl")
scaler = joblib.load(f"{BASE_PATH}/MACHINE_LEARNING/scaler.pkl")

spark = SparkSession.builder \
    .appName("Traffic Stream") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .load()
df = df.selectExpr("CAST(value AS STRING)")

schema = """
Region DOUBLE,
`Site name` DOUBLE,
Detector DOUBLE,
Hour DOUBLE,
Day DOUBLE,
Peak_Hour DOUBLE,
`Average Traffic_Volume` DOUBLE,
Actual DOUBLE,
`Predicted Congestion` DOUBLE,
`Date Time` STRING
"""

df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

def predict(batch_df, epoch_id):

    pdf = batch_df.toPandas()

    if len(pdf) == 0:
        return

    # Create features
    X = pd.DataFrame({
        'Region': pdf['Region'],
        'Site': pdf['Site name'],
        'Detector': pdf['Detector'],
        'Hour': pdf['Hour'],
        'Day': pdf['Day'],
        'Peak_Hour': pdf['Peak_Hour'],
        'Avg_Volume': pdf['Average Traffic_Volume']
    })

    # Important correction
    X_scaled = scaler.transform(X)

    # Predict
    pdf['Prediction'] = model.predict(X_scaled)

    print("\n===== NEW BATCH =====")

    print(
        pdf[['Region',
             'Site name',
             'Detector',
             'Hour',
             'Day',
             'Peak_Hour',
             'Average Traffic_Volume',
             'Actual',
             'Prediction',
             'Date Time']]
    )

query = df.writeStream \
    .foreachBatch(predict) \
    .option("checkpointLocation", f"{BASE_PATH}/BIG_DATA/checkpoint") \
    .start()

query.awaitTermination()