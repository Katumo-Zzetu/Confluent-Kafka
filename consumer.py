import os
import json
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Kafka configuration
conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
    'group.id': 'weather-consumer-group',
    'auto.offset.reset': 'latest'
}

# PostgreSQL URL
pg_url = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

# Connect to PostgreSQL
engine = create_engine(pg_url)

# Ensure the table exists
with engine.connect() as conn:
    conn.execute(text("""
        DROP TABLE IF EXISTS weather_data;
        CREATE TABLE weather_data (
            "city" TEXT,
            "temp" DOUBLE PRECISION,
            "feels_like" DOUBLE PRECISION,
            "humidity" INTEGER,
            "pressure" INTEGER,
            "wind_speed" DOUBLE PRECISION,
            "weather_main" TEXT,
            "weather_desc" TEXT,
            "timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.commit()
# Set up Kafka consumer
consumer = Consumer(conf)
consumer.subscribe(['weather_data'])

print("⛅ Listening to Kafka topic 'weather_topic'...")

try:
 while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))

            df = pd.DataFrame([{
                "city": data.get("city"),
                "temp": data.get("temp"),
                "feels_like": data.get("feels_like"),
                "humidity": data.get("humidity"),
                "pressure": data.get("pressure"),
                "wind_speed": data.get("wind_speed"),
                "weather_main": data.get("weather_main"),
                "weather_desc": data.get("weather_desc")
            }])

            df.to_sql("weather_data", engine, if_exists="append", index=False)
            print(f"✅ Inserted weather data for city: {data.get('city')}")
        except Exception as e:
            print(f"⚠️ Error processing message: {e}")

except KeyboardInterrupt:
    print("\n🛑 Stopped by user")

finally:
    consumer.close()
