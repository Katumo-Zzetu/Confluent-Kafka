import requests
import json
import time
from kafka import KafkaProducer

API_KEY = "<API-KEY>"
CITIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru"]

# Replace these with your Confluent Cloud credentials
BOOTSTRAP_SERVERS = "<BOOTSTRAP-SERVER>"
KAFKA_API_KEY = "<API-KEY>"
KAFKA_API_SECRET = "<API-SECRET>"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_API_KEY,
    sasl_plain_password=KAFKA_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    res = requests.get(url)
    d = res.json()
    return {
        "city": d["name"],
        "temp": d["main"]["temp"],
        "feels_like": d["main"]["feels_like"],
        "humidity": d["main"]["humidity"],
        "pressure": d["main"]["pressure"],
        "wind_speed": d["wind"]["speed"],
        "weather_main": d["weather"][0]["main"],
        "weather_desc": d["weather"][0]["description"]
    }

while True:
    for city in CITIES:
        try:
            data = fetch_weather(city)
            producer.send("weather_data", value=data)
            print(f"✅ Sent: {data}")
        except Exception as e:
            print(f"❌ Error with {city}: {e}")
    time.sleep(10)


