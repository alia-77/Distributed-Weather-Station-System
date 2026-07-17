# Distributed Weather Station System

A distributed weather monitoring system built for a university Distributed Systems course. Multiple simulated weather stations stream sensor readings through Kafka to a central aggregator, with a stream-processing service detecting rain conditions in real time. The whole system is containerized and deployable to Kubernetes.

## Architecture

```
WeatherStation (producer)  →  Kafka topic: weather-readings
                                     │
                    ┌────────────────┼─────────────────┐
                    ▼                                   ▼
         CentralStation (consumer)          RainDetector (Kafka Streams)
         batches + writes to Postgres        filters humidity > 70%
                                              → Kafka topic: rain-alerts
```

- **WeatherStation** — simulates a physical station: generates temperature, humidity, wind speed and battery status readings on an interval, publishes them as JSON to Kafka. Includes randomized packet drops to simulate unreliable sensors, and derives a station ID from its pod name so replicas identify themselves automatically.
- **CentralStation** — consumes readings, batches inserts (configurable batch size), and writes them to a PostgreSQL table it initializes on startup. Uses manual offset commits so a batch is only acknowledged once it's durably written.
- **RainDetector** — a Kafka Streams topology that filters the reading stream for high humidity and republishes matches to a `rain-alerts` topic, decoupling alerting from storage.

## Stack
Java, Apache Kafka (producer/consumer + Kafka Streams), PostgreSQL, Docker, Kubernetes

## Deployment

Kubernetes manifests are in `k8s/`: separate deployments for Kafka, Postgres (with a PVC for persistence), the central station, weather stations, and the rain detector, plus a ConfigMap/Secret for shared configuration.

```bash
docker-compose up          # local run
kubectl apply -f k8s/       # cluster deployment
```

## Design notes

- Services are configured entirely through environment variables (Kafka bootstrap servers, DB credentials, batch size, send interval), so the same image runs locally via Docker Compose or in Kubernetes without rebuilding.
- Batched, transactional writes to Postgres, paired with manual Kafka offset commits, are used to avoid losing readings if the central station crashes mid-batch.
- Rain detection is implemented as a separate Kafka Streams service rather than logic inside the consumer, so alerting can scale and evolve independently of storage.
