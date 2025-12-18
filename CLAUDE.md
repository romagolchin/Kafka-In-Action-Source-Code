# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the source code repository for the "Kafka in Action" book. It contains Java examples demonstrating Apache Kafka concepts organized by book chapters. The codebase is educational, showing patterns for producers, consumers, Kafka Streams, and integration scenarios.

**Important**: This code is NOT production-ready. Dependencies may have security vulnerabilities over time. See https://kafka.apache.org/cve-list for security updates.

## Technology Stack

- **Java**: 11 (required)
- **Apache Kafka**: 2.7.1
- **Scala version**: 2.13
- **Avro**: 1.10.2
- **Confluent Platform**: 6.1.1
- **Build Tool**: Maven 3.6.x (Maven Wrapper provided)

## Repository Structure

The repository is organized as a Maven multi-module project with one module per chapter:

- `KafkaInAction_Chapter2` through `KafkaInAction_Chapter12`: Chapter-specific examples
- `KafkaInAction_AppendixB`: Appendix examples
- Each chapter module contains:
  - `src/main/java/org/kafkainaction/`: Java source code
  - `Commands.md` or `Commands.adoc`: Shell commands referenced in that chapter
  - Chapter-specific `pom.xml` inheriting from root POM

### Common Package Structure

Most chapters follow this pattern:
- `org.kafkainaction.producer`: Kafka producer examples
- `org.kafkainaction.consumer`: Kafka consumer examples
- `org.kafkainaction.model`: Domain models (e.g., Alert)
- `org.kafkainaction.serde`: Custom serializers/deserializers
- `org.kafkainaction.partitioner`: Custom partitioners
- `org.kafkainaction.callback`: Callback implementations

Some code is reused across chapters rather than duplicated.

## Build Commands

### Build entire project
```bash
./mvnw verify
```

### Build specific chapter
```bash
./mvnw --projects KafkaInAction_Chapter12 verify
```

### Clean build
```bash
./mvnw clean verify
```

### Create JAR with dependencies
The maven-assembly-plugin is configured to create fat JARs with all dependencies included during the package phase.

## Running Kafka Locally

### Using Docker Compose (Recommended)

The `docker-compose.yaml` at the root provides a complete Kafka environment:
- 1 ZooKeeper instance
- 3 Kafka brokers (ports 9092, 9093, 9094)
- Confluent Schema Registry (port 8081)
- ksqlDB server (port 8088)
- ksqlDB CLI

```bash
docker-compose up -d
docker-compose down
```

### Manual Installation (Alternative)

If running Kafka manually without Docker:

1. Extract Kafka: `tar -xzf kafka_2.13-2.7.1.tgz && cd kafka_2.13-2.7.1`
2. Start ZooKeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties`
3. Configure and start 3 brokers with configs in `config/server{0,1,2}.properties`
   - broker.id: 0, 1, 2
   - listeners: localhost:9092, localhost:9093, localhost:9094
   - log.dirs: /tmp/kafkainaction/kafka-logs-{0,1,2}
4. Start brokers: `bin/kafka-server-start.sh config/server0.properties` (repeat for 1 and 2)

Helper scripts in `KafkaInAction_Chapter2/src/main/resources`:
- `starteverything.sh`: Start ZooKeeper and all brokers (requires first-time setup)
- `stopeverything.sh`: Stop ZooKeeper and brokers
- `portInUse.sh`: Kill processes using Kafka ports

To stop: `bin/kafka-server-stop.sh` then `bin/zookeeper-server-stop.sh`

## Running Examples

Most examples can be run from IDE or command line. Ensure ZooKeeper and Kafka brokers are running first.

Examples typically:
1. Write data to Kafka topics
2. Print results to console
3. May require topics to be created first (see Commands.md in each chapter)

### Example topic creation
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kinaction_alert --partitions 3 --replication-factor 3
```

## IDE Setup

### Eclipse
From repository root:
```bash
mvn eclipse:eclipse
```
Or use File → Import → Existing Maven Projects

### IntelliJ IDEA
Open the root `pom.xml` as a project. IntelliJ will automatically detect the multi-module Maven structure.

## Key Architectural Patterns

### Producer Patterns
- **Fire-and-forget**: Send without waiting for acknowledgment
- **Synchronous send**: Block until acknowledgment received
- **Asynchronous send with callbacks**: Non-blocking with result handling
- **Idempotent producers**: `enable.idempotence=true` to prevent duplicates
- **Custom partitioners**: Extend `Partitioner` to control partition assignment (e.g., `AlertLevelPartitioner` routes critical alerts to partition 0)

### Consumer Patterns
- **Auto-commit offsets**: `enable.auto.commit=true` (default, may lose messages on failure)
- **Manual synchronous commit**: `commitSync()` after processing
- **Manual asynchronous commit**: `commitAsync()` with callback for better performance
- **Partition assignment**: Automatic via consumer groups or manual via `assign()`
- **Offset reset strategies**: `auto.offset.reset=earliest|latest`

### Data Formats
- **Avro with Schema Registry**: Used in several examples for schema evolution
- **Custom SerDes**: See `AlertKeySerde` for custom serialization examples

### Kafka Streams Examples
- Located primarily in Chapter 12 (`kstreams2` package)
- Demonstrates KStream/KTable operations
- Shows stateful processing with custom transformers and processors

### Testing
Some chapters include integration tests using:
- `EmbeddedKafkaCluster` for in-memory testing
- TestContainers (alternative approach)

## Common Configuration Notes

### bootstrap.servers
Only needs to list one broker; clients will discover the rest via metadata requests.

### acks setting
- `0`: No acknowledgment (fastest, least durable)
- `1`: Leader acknowledgment only
- `all`: All in-sync replicas must acknowledge (most durable)

### Consumer groups
Use different `group.id` values to have multiple independent consumers process the same data.

## Useful Kafka CLI Commands

These commands assume Kafka is running on localhost:9092

### List topics
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Describe topic
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <topic-name>
```

### Console producer
```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic <topic-name>
```

### Console consumer (from beginning)
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning
```

### Consumer groups
```bash
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group-name>
```

## Notes for Development

- Code examples are intentionally simplified for educational purposes. Not all commands and code snippets from the book are included.
- The "Kafka in action.md" file contains detailed notes in Russian covering Kafka concepts, architecture, and patterns.
- Each chapter's `Commands.md` contains the specific Kafka commands referenced in that chapter.
- Some dependencies may have known vulnerabilities - this is expected for educational code frozen at a specific version.
