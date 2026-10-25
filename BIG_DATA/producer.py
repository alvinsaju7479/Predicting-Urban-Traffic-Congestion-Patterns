from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime

# Read CSV
BASE_PATH = "/Users/alvinsaju/Desktop/DISSERTATION/Predicting Urban Traffic Congestion Patterns"
df = pd.read_csv(f"{BASE_PATH}/MACHINE_LEARNING/data.csv")

df = df[df['Average Traffic_Volume'] > 0]

# Create Date Time column
df['Date Time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Select columns
df = df[['Region',
         'Site name',
         'Detector',
         'Hour',
         'Day',
         'Peak_Hour',
         'Average Traffic_Volume',
         'Actual',
         'Predicted Congestion',
         'Date Time']]

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send rows
for _, row in df.iterrows():

    # Update live time for every row
    row['Date Time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = {
        'Region': float(row['Region']),
        'Site name': float(row['Site name']),
        'Detector': float(row['Detector']),
        'Hour': float(row['Hour']),
        'Day': float(row['Day']),
        'Peak_Hour': float(row['Peak_Hour']),
        'Average Traffic_Volume': float(row['Average Traffic_Volume']),
        'Actual': float(row['Actual']),
        'Predicted Congestion': float(row['Predicted Congestion']),
        'Date Time': row['Date Time']
    }

    producer.send('traffic-data', value=data)

    

    print("Sent:", data)

    time.sleep(5)
producer.flush()