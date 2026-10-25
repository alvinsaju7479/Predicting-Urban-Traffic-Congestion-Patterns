from kafka import KafkaConsumer
import json
import gspread
import time

from oauth2client.service_account import ServiceAccountCredentials
BASE_PATH = "/Users/alvinsaju/Desktop/DISSERTATION/Predicting Urban Traffic Congestion Patterns"
# Google Sheets setup
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    f"{BASE_PATH}/BIG_DATA/credentials.json",
    scope
)

client = gspread.authorize(creds)

sheet = client.open("Kafka_Stream_Data").sheet1

# Kafka Consumer
consumer = KafkaConsumer(
    'traffic-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("Listening to Kafka...")

for message in consumer:
    data = message.value
    # Create ordered row
    row = [
        data.get('Region'),
        data.get('Site name'),
        data.get('Detector'),
        data.get('Hour'),
        data.get('Day'),
        data.get('Peak_Hour'),
        data.get('Average Traffic_Volume'),
        data.get('Actual'),
        data.get('Predicted Congestion'),
        data.get('Date Time')
    ]
    try:
        sheet.append_row(row)
        print("Inserted:", row)
        time.sleep(5)
    except Exception as e:
        print("Error:", e)
        time.sleep(10)