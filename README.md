# Stock Market Streaming Analytics – Assignment 3

## Project Overview
End-to-end streaming analytics pipeline for HDFCBANK, INFY, RELIANCE and TCS.

**Flow:** Sample Data → `producer.py` → Kafka (`stock-prices`) → `consumer.py` → MySQL (`stockdb.stock_market_data`) → Grafana

## Objectives
- Stream stock records through Kafka.
- Calculate price change, percentage change and MA(5).
- Generate rule-based alerts such as High volume and significant price movement.
- Store processed records in MySQL.
- Visualize trends, volume and alerts in Grafana.

## Technologies
Python, Apache Kafka, Docker, MySQL 8, Grafana, kafka-python, mysql-connector-python.

## Main Database Fields
`event_timestamp`, `symbol`, `price`, `previous_price`, `volume`, `price_change`, `percentage_change`, `moving_average`, `alert_status`, `timestamp`.

## Dashboard Panels
1. **Stock Price vs MA(5):** Time-series comparison of price and 5-period moving average with a stock selector.
2. **Price Change % vs Trading Volume:** Dual-axis time-series chart; Price Change % on the left axis and Volume on the right.
3. **Streaming Alert Analysis:** Bar chart of Normal, High volume and significant price-movement events.
4. **Latest Streaming Metrics:** Table of latest price, MA(5), price change, percentage change, volume and alert status.

## How to Run

### Install dependencies
```powershell
pip install -r requirements.txt
```

### Verify Kafka topic
```powershell
docker exec -it sda-kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092
```
Confirm `stock-prices` exists.

### Start producer
```powershell
python producer.py
```

### Start consumer
In another terminal:
```powershell
python consumer.py
```

### Grafana
Open:
`http://localhost:3000`

MySQL connection:
- Host: `127.0.0.1:3307`
- Database: `stockdb`
- User: `root`
- Password: project MySQL password

## Business Insights
The Price vs MA(5) chart helps identify short-term trend changes and potential crossover situations. The Price Change % vs Volume chart adds context to price movements by showing whether increased activity accompanies a movement. The alert panel helps an analyst focus on unusual events instead of manually inspecting every incoming record.

## Project Files
- `producer.py`
- `consumer.py`
- `requirements.txt`
- `README.md`
- `dashboard_queries.sql`
- `SDA_Assignment_3_Final_Report.docx`
- `screenshots/`

This project is an academic demonstration using simulated/replayed stock data and is not intended as a live trading system.
