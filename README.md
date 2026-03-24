# Environmental Sensor Streaming Pipeline
This project implements a real-time data processing pipeline for environmental sensor data. Measurements (e.g., CO, LPG, smoke, temperature, humidity) of three IoT devices placed in different locations with varying environmental conditions are streamed to Apache Kafka. The data is processed using a stream processor that computes a composite environmental index, determines a status, and creates an alert in the case of a status change, thereby making aware of abnormal conditions. Processed data and alerts are stored in PostgreSQL and visualized in Grafana through an interactive dashboard.

The system enables:
- Real-time monitoring of environmental conditions
- Detection of abnormal conditions based on a computed environmental index
- Creation of environmental alerts
- Visualization of trends and alerts


## Contents

- [Architecture](#architecture)
- [Dataset](#dataset)
- [Requirements](#requirements)
- [Setup & Usage](#setup-&-usage)
- [Dashboard Features](#dashboard-features)
- [Environmental Index](#environmental-index)
- [Alert Logic](#alert-logic)
- [Project Structure](#project-structure)
- [Notes](#notes)


## Architecture
The pipeline consists of the following components:

- **Producers**: Simulate IoT devices streaming sensor data from a dataset
- **Kafka**: Handles real-time data streaming
- **Processor**: Computes environmental index, detects status changes, and creates alerts
- **Consumer and Alert Consumer**: Consume processed data and alerts from Kafka and store them in PostgreSQL
- **PostgreSQL**: Stores processed data and alerts
- **Grafana**: Visualizes data in a dashboard

Data flow:
```console
Producer → Kafka → Processor → Kafka → Consumer/Alert Consumer → PostgreSQL → Grafana
```

## Dataset
The project uses the *Environmental Sensor Telemetry Data* dataset:

```console
https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k
```

Sensors include:
- CO (carbon monoxide)
- LPG
- Smoke
- Temperature
- Humidity
- Light
- Motion


## Requirements
- Docker
- Docker Compose


## Setup & Usage

### 1. Download dataset
Download and extract the dataset from Kaggle.

### 2. Prepare data folder
Create a folder:
```console
producer/data/
```

Move the file:
```console
iot_telemetry_data.csv
```

into that folder.


### 3. Start the system
Run:
```console
docker compose up --build
```

This will start:
- Kafka
- Producers
- Stream processor
- Consumer
- Alert consumer
- PostgreSQL
- Grafana


### 4. Open Grafana
Go to:
```console
http://localhost:3000/
```

Login:
```console
Username: admin
Password: admin
```
(Changing the password can be skipped)


### 5. Import dashboard
- Click **+** (upper right-hand corner) **→ Import dashboard**
- Upload:
```console
grafana/dashboards/environmental_dashboard.json
```
- Select a PostgreSQL data source:
```console
grafana-postgresql-datasource
```
- Click **Import**


## Dashboard Features

The Grafana dashboard includes three panels:

- **Current Environmental Index Overview** (per location)
- **Environmental Alerts** (showing status changes)
- **Environmental Index Trend** (time-series visualization)


## Environmental Index

A composite environmental index is calculated using:
```console
0.4 × CO + 0.3 × LPG + 0.3 × Smoke
```

This simplifies multiple sensor readings into a single indicator.


## Alert Logic

Alerts are triggered when the environmental index crosses thresholds:

- NORMAL
- WARNING
- CRITICAL

To avoid rapid fluctuations:
- Moving averages are used
- Hysteresis is applied


## Project Structure
```console
project/
│
├── producer/
├── processor/
├── consumer/
├── alert_consumer/
├── grafana/
│ ├── dashboards/
│ └── provisioning/
├── docker-compose.yml
└── README.md
```


## Notes

- The PostgreSQL data source is configured automatically via Grafana provisioning.
- Data is streamed continuously from the CSV file to simulate real-time IoT devices. As soon as the end of the file is reached, the producer starts reading from the top again.
- Each device represents a different urban environment (park, residential, and traffic area).
- Alerts are triggered when the environmental index crosses predefined thresholds:  
  0.015 → WARNING  
  0.020 → CRITICAL  

- Hysteresis is applied to prevent rapid alert fluctuations near threshold values.  

  Example:
  1. Index ≥ 0.015 → WARNING alert is sent.
  2. Status is only set back to NORMAL if index < 0.0145.
