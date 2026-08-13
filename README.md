# Distributed Weather Station System

A distributed weather monitoring system where simulated weather stations publish sensor readings to Kafka, while independent services process, store, and analyze the data. The system supports both local Docker Compose deployment and Kubernetes.

## Architecture

```text
WeatherStation
      │
      ▼
Kafka: weather-readings
      │
      ├───────────────┐
      ▼               ▼
CentralStation    RainDetector
      │            (Kafka Streams)
      ▼               │
PostgreSQL            ▼
                  Kafka: rain-alerts
```

## Services

* **WeatherStation** - Simulates weather stations and publishes temperature, humidity, wind speed, battery status, timestamps, and sequence numbers to Kafka. Derives a station ID from its pod name so replicas identify themselves automatically. Also simulates unreliable sensors by randomly dropping readings.
* **CentralStation** - Consumes weather readings from Kafka, batches them (configurable batch size), and writes them to a PostgreSQL table it initializes on startup. Kafka offsets are manually committed only after a batch is successfully written, so no readings are lost if it crashes mid-batch.
* **RainDetector** - Uses Kafka Streams to filter readings with humidity above 70% and publishes them to the `rain-alerts` topic, keeping alerting separate from storage.

## Tech Stack

* Java 17
* Apache Kafka
* Kafka Streams
* PostgreSQL
* Docker
* Kubernetes
* Maven
* Jackson

## Running Locally

Start Kafka and PostgreSQL:

```bash
docker-compose up
```

Build the Java application:

```bash
mvn clean package
```

The same Docker image runs any of the services by setting the `MAIN_CLASS` environment variable.

## Kubernetes

Kubernetes manifests are in `k8s/`: separate deployments for Kafka, Postgres (with a PVC for persistence), WeatherStation, CentralStation, and RainDetector, plus a ConfigMap/Secret for shared config.

```bash
kubectl apply -f k8s/
```

## Key Design Points

* Kafka decouples weather stations from the processing services.
* Multiple weather stations can run independently.
* CentralStation uses batched PostgreSQL writes and manual Kafka offset commits for durability.
* RainDetector is a separate Kafka Streams service so alerting can scale and evolve independently of storage.
* Services are configured entirely through environment variables, so the same containers run locally or in Kubernetes without rebuilding.