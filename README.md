# Environmental Sensor Streaming Pipeline
This project implements a real-time data processing system for environmental monitoring in a city.  
IoT devices continuously send sensor data, which is processed using a streaming pipeline and visualized in Grafana.

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
- Go to **Dashboards → Import**
- Upload:
```console
grafana/dashboards/environmental_dashboard.json
```
- Select the PostgreSQL datasource


## Dashboard Features

The Grafana dashboard includes:

- **Environmental Index Status** (per location)
- **Alert history** showing status changes
- **Time-series visualization** of environmental index


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
├── grafana/
│ ├── dashboards/
│ └── provisioning/
├── docker-compose.yml
└── README.md
```


## Notes

- Data is streamed continuously to simulate real-time IoT devices
- Grafana datasource is automatically provisioned
- Dashboard must be imported manually
