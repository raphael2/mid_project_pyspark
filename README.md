# Spark & Kafka Data Pipeline

## Overview
This project demonstrates a Spark & Kafka-based data pipeline for vehicle event processing,enrichment, and alert detection. 
The pipeline consists of multiple stages that include generating car data, streaming sensor data to Kafka,
enriching it with additional metadata, and detecting anomalies.

## Project Structure

```
|-- ex1_car_models.py          # Defines car models and stores them in Parquet
|-- ex2_car_colors.py          # Defines car colors and stores them in Parquet
|-- ex3_car_generator.py       # Generates synthetic car data
|-- ex4_data_generator.py      # Streams car sensor data to Kafka
|-- ex5_data_enrichment.py     # Enriches incoming Kafka data with metadata
|-- ex6_alert_detection.py     # Detects alerts based on predefined conditions
|-- ex7_alert_counter.py       # Aggregates and counts alerts over a time window
```

## Exercises Breakdown

### 1. Car Models Creation (`ex1_car_models.py`)
- Defines a structured dataset of car models.
- Uses Apache Spark to create and store data in Parquet format.
- Output location: `s3a://spark/data/dims/car_models`.

### 2. Car Colors Creation (`ex2_car_colors.py`)
- Defines available car colors.
- Uses Apache Spark to create and store data in Parquet format.
- Output location: `s3a://spark/data/dims/car_colors`.

### 3. Car Generator (`ex3_car_generator.py`)
- Generates a synthetic dataset of cars with unique identifiers.
- Includes attributes such as `car_id`, `driver_id`, `model_id`, and `color_id`.
- Output location: `s3a://spark/data/dims/cars`.

### 4. Data Generator (`ex4_data_generator.py`)
- Reads car data and generates real-time sensor events.
- Publishes events to Kafka topic `sensors-sample`.
- Attributes:
  - `event_id`: Unique event identifier.
  - `event_time`: Timestamp.
  - `car_id`: Vehicle identifier.
  - `speed`: Random speed (0-200 km/h).
  - `rpm`: Random RPM (0-8000).
  - `gear`: Random gear (1-7).
- Runs continuously with a `while True` loop and 1-second interval.

### 5. Data Enrichment (`ex5_data_enrichment.py`)
- Reads sensor events from Kafka (`sensors-sample`).
- Enriches data with additional attributes from reference datasets.
- Computes `expected_gear` as `round(speed / 30)`.
- Publishes enriched data to Kafka topic `samples-enriched`.

### 6. Alert Detection (`ex6_alert_detection.py`)
- Reads enriched data from Kafka (`samples-enriched`).
- Filters events based on alert conditions:
  - Speed > 120 km/h.
  - Expected gear does not match actual gear.
  - RPM > 6000.
- Publishes alerts to Kafka topic `alert-data`.

### 7. Alert Counter (`ex7_alert_counter.py`)
- Reads alert data from Kafka (`alert-data`).
- Aggregates and counts alerts over a rolling 15-minute window.
- Outputs summary statistics to the console:
  - Total alerts.
  - Count of black, white, and silver cars.
  - Maximum speed, gear, and RPM recorded.

## Running the Pipeline
### Prerequisites
- Apache Spark
- Kafka
- Python 3.x
- Dependencies: `pyspark`, `kafka-python`

### Execution Order
1. Run `ex1_car_models.py` and `ex2_car_colors.py` to create dimension tables.
2. Run `ex3_car_generator.py` to generate car data.
3. Start Kafka broker and topic setup.
4. Run `ex4_data_generator.py` to start streaming sensor data to Kafka.
5. Run `ex5_data_enrichment.py` to process and enrich incoming events.
6. Run `ex6_alert_detection.py` to detect alerts and push them to Kafka.
7. Run `ex7_alert_counter.py` to monitor alert statistics.
 

## Authors
Developed as part of a pySpark mid project .

