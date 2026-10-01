"""
Assignment 3: Kafka Consumer -> MySQL -> Grafana

Reads JSON stock messages from Kafka topic `stock-prices`,
calculates simple streaming analytics, and stores them in MySQL.

Current Docker setup:
Kafka: localhost:9092
MySQL host port: 3307
MySQL database: stockdb
MySQL user: root

Do NOT put your MySQL password in this file.
Set MYSQL_PASSWORD in PowerShell before running:
$env:MYSQL_PASSWORD="YOUR_PASSWORD"
python consumer.py
"""

import json
import os
from collections import defaultdict, deque

import mysql.connector
from kafka import KafkaConsumer


# -----------------------------
# Configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stock-prices"
KAFKA_GROUP_ID = "assignment3-grafana"

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = "stockdb"

MA_WINDOW = 5

# Keep the latest 5 prices separately for each stock.
price_history = defaultdict(lambda: deque(maxlen=MA_WINDOW))


# -----------------------------
# MySQL connection and table
# -----------------------------
def connect_mysql():
    if not MYSQL_PASSWORD:
        raise RuntimeError(
            "MYSQL_PASSWORD is not set. In PowerShell run:\n"
            '$env:MYSQL_PASSWORD="YOUR_PASSWORD"\n'
            "Then run: python consumer.py"
        )

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )

    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_market_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_timestamp DATETIME NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            price DECIMAL(12,2) NOT NULL,
            previous_price DECIMAL(12,2) NOT NULL,
            volume INT NOT NULL,
            price_change DECIMAL(12,2) NOT NULL,
            percentage_change DECIMAL(10,4) NOT NULL,
            moving_average DECIMAL(12,2) NOT NULL,
            alert_status VARCHAR(100) NOT NULL
        )
    """)

    connection.commit()
    cursor.close()

    return connection


# -----------------------------
# Streaming calculations
# -----------------------------
def calculate_analytics(message):
    symbol = message["symbol"]

    price = float(message["price"])
    previous_price = float(message["previous_price"])
    volume = int(message["volume"])

    price_history[symbol].append(price)

    moving_average = (
        sum(price_history[symbol]) / len(price_history[symbol])
    )

    price_change = price - previous_price

    percentage_change = (
        (price_change / previous_price) * 100
        if previous_price != 0
        else 0
    )

    alerts = []

    if abs(percentage_change) > 2:
        alerts.append("Price movement > 2%")

    if volume > 15000:
        alerts.append("High volume")

    alert_status = "; ".join(alerts) if alerts else "Normal"

    return {
        "price_change": round(price_change, 2),
        "percentage_change": round(percentage_change, 4),
        "moving_average": round(moving_average, 2),
        "alert_status": alert_status,
    }


# -----------------------------
# Main consumer
# -----------------------------
def main():
    print("=" * 75)
    print("ASSIGNMENT 3 - STOCK MARKET STREAMING CONSUMER")
    print("=" * 75)

    print(f"Kafka broker : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic  : {KAFKA_TOPIC}")
    print(f"MySQL host   : {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"MySQL DB     : {MYSQL_DATABASE}")
    print()

    print("Connecting to MySQL...")
    db = connect_mysql()
    cursor = db.cursor()
    print("MySQL connection successful.")

    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    print("Kafka connection successful.")
    print("-" * 75)
    print("Consumer is running. Waiting for stock messages...")
    print("-" * 75)

    try:
        for record in consumer:
            message = record.value

            analytics = calculate_analytics(message)

            cursor.execute("""
                INSERT INTO stock_market_data (
                    event_timestamp,
                    symbol,
                    price,
                    previous_price,
                    volume,
                    price_change,
                    percentage_change,
                    moving_average,
                    alert_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                message["timestamp"],
                message["symbol"],
                message["price"],
                message["previous_price"],
                message["volume"],
                analytics["price_change"],
                analytics["percentage_change"],
                analytics["moving_average"],
                analytics["alert_status"],
            ))

            db.commit()

            print(
                f"Consumed | "
                f"{message['symbol']:<9} | "
                f"Price: {float(message['price']):>8.2f} | "
                f"Change: {analytics['percentage_change']:>7.3f}% | "
                f"MA(5): {analytics['moving_average']:>8.2f} | "
                f"Volume: {int(message['volume']):>5} | "
                f"Alert: {analytics['alert_status']}"
            )

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        cursor.close()
        db.close()
        consumer.close()
        print("Connections closed.")


if __name__ == "__main__":
    main()
