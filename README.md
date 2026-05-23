# Kafka Distributed Workers

Distributed task processing system using **Spring Boot** and **Apache Kafka**.  
Demonstrates real parallel processing, consumer groups, partition distribution, rebalance, and horizontal scaling.

---

## Architecture

```
                ┌──────────────────────┐
                │  PostgreSQL          │
                │  TASK table          │
                │  (PENDING/PROCESSING │
                │   /DONE)             │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │  Producer            │
                │  @Scheduled          │
                │  (every 5 seconds)   │
                │                      │
                │  1. SELECT PENDING   │
                │  2. CAS → PROCESSING │
                │  3. Send ID to Kafka │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────────────────┐
                │  Kafka Broker (KRaft, no ZK)     │
                │                                  │
                │  Topic: task-processing           │
                │  ┌────┬────┬────┬────┬────┬────┐ │
                │  │ P0 │ P1 │ P2 │ P3 │ P4 │ P5 │ │
                │  └────┴────┴────┴────┴────┴────┘ │
                └──────────┬───────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼                             ▼
   ┌─────────────────┐          ┌─────────────────┐
   │  worker-pod-1   │          │  worker-pod-2   │
   │                 │          │                 │
   │  Consumer T-0   │          │  Consumer T-0   │
   │   → P0          │          │   → P3          │
   │  Consumer T-1   │          │  Consumer T-1   │
   │   → P1          │          │   → P4          │
   │  Consumer T-2   │          │  Consumer T-2   │
   │   → P2          │          │   → P5          │
   └─────────────────┘          └─────────────────┘
```

---

## Core Flow

```
1. Scheduler reads PENDING tasks from PostgreSQL
2. Atomically marks each as PROCESSING (compare-and-swap)
3. Publishes task ID to Kafka topic "task-processing"
4. Kafka distributes messages across 6 partitions (key = task ID)
5. Consumer threads receive task IDs from their assigned partitions
6. Consumer loads task from DB, simulates processing, marks as DONE
7. Offset is committed → message will not be redelivered
```

---

## Distributed Systems Concepts

### Consumer Group

All consumers with `groupId = "workers"` form a **consumer group**.  
Kafka treats them as a single logical subscriber:

- Each message is delivered to **exactly one** consumer in the group
- Partitions are distributed among group members
- Adding/removing consumers triggers automatic **rebalance**

```
Consumer Group: "workers"
├── Instance-1/Thread-0  →  Partition 0
├── Instance-1/Thread-1  →  Partition 1
├── Instance-1/Thread-2  →  Partition 2
├── Instance-2/Thread-0  →  Partition 3
├── Instance-2/Thread-1  →  Partition 4
└── Instance-2/Thread-2  →  Partition 5
```

### Partitions = Unit of Parallelism

**Partitions are the unit of parallelism. NOT messages.**

- A topic with 6 partitions supports **at most 6 parallel consumers** in one group
- Each partition is assigned to **exactly one** consumer per group
- If consumers > partitions, excess consumers sit idle
- If consumers < partitions, some consumers handle multiple partitions

```
6 partitions + 2 consumers → 3 partitions each
6 partitions + 3 consumers → 2 partitions each
6 partitions + 6 consumers → 1 partition each (max parallelism)
6 partitions + 9 consumers → 6 active + 3 idle
```

### Partition Ownership

Once Kafka assigns a partition to a consumer, that consumer is the **exclusive owner**:

- No other consumer in the same group reads from that partition
- No database locks needed for concurrency control
- No distributed mutex needed
- Kafka coordinates everything via the **group protocol**

### Offset and Commit

Each message within a partition has a sequential **offset** (0, 1, 2, ...):

```
Partition 0: [msg@0] [msg@1] [msg@2] [msg@3] [msg@4]
                                       ↑
                              committed offset = 3
                              next poll returns msg@3
```

- **Commit**: tells Kafka "I processed up to offset X"
- If consumer crashes before commit → message is redelivered (at-least-once)
- `ack-mode: record` = commit after each individual message
- `enable-auto-commit: false` = application controls when to commit

### Rebalance

Triggered automatically when:

1. **Consumer joins** the group (new pod starts, new thread)
2. **Consumer leaves** the group (pod crashes, graceful shutdown)
3. **Partitions change** (topic reconfigured)

During rebalance:

```
1. Kafka pauses all consumers in the group
2. Group coordinator recalculates partition assignments
3. New assignments are pushed to consumers
4. Consumers resume from last committed offset
5. Processing continues — no data loss
```

**Example — pod failure and recovery:**

```
BEFORE: 2 instances, 6 partitions
  Instance-1 → P0, P1, P2
  Instance-2 → P3, P4, P5

Instance-2 crashes → rebalance triggered

AFTER: 1 instance, 6 partitions
  Instance-1 → P0, P1, P2, P3, P4, P5
  (all partitions reassigned to surviving instance)

Instance-2 restarts → rebalance triggered again

AFTER: 2 instances, 6 partitions
  Instance-1 → P0, P1, P2
  Instance-2 → P3, P4, P5
  (partitions redistributed evenly)
```

### Why Kafka Avoids Database Contention

Traditional approach (without Kafka):
```
Instance-1: SELECT ... FOR UPDATE → process → UPDATE → release lock
Instance-2: SELECT ... FOR UPDATE → BLOCKED waiting for lock
```

Kafka approach:
```
Instance-1: poll(partition 0) → process → commit offset
Instance-2: poll(partition 1) → process → commit offset
(completely independent — zero contention)
```

Each consumer reads from **its own partitions**. No shared state. No locks. No contention.

### Horizontal Scaling

```
Scale UP:   docker compose up --scale app=3
            → 3 new consumers join group
            → rebalance: partitions redistributed
            → more parallelism

Scale DOWN: docker compose up --scale app=1
            → 2 consumers leave group
            → rebalance: remaining instance gets all partitions
            → processing continues uninterrupted
```

---

## Responsibilities

### Kafka DOES:
- Distribute messages across partitions
- Assign partitions to consumers
- Coordinate consumer group membership
- Manage offsets (track what was processed)
- Trigger rebalance on group changes
- Guarantee ordering within each partition

### Kafka DOES NOT:
- Create Kubernetes pods
- Scale infrastructure
- Execute business logic
- Manage database transactions

### Kubernetes (or Docker Compose) DOES:
- Start/stop containers
- Restart failed pods
- Scale replicas up/down
- Health checks

### The Application DOES:
- Read pending tasks from database
- Publish task IDs to Kafka
- Consume messages and process tasks
- Commit offsets after successful processing

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- (Optional) Java 17 + Maven for local development

### 1. Start with a single instance

```bash
docker compose up --build
```

This starts:
- **Kafka** (KRaft mode, no ZooKeeper) on port 9092
- **PostgreSQL** on port 5432
- **Kafka UI** at http://localhost:8090
- **1 app instance** (producer + consumer)

### 2. Watch the logs

```bash
docker compose logs -f app
```

Expected output:
```
app-1  | Inserting 100 fake tasks...
app-1  | 100 tasks inserted successfully with status=PENDING
app-1  | [abc123] Found 100 PENDING tasks
app-1  | [abc123] Published 100 tasks to Kafka
app-1  | [abc123] [workers-0-C-1] Partition=0 Offset=0 | Received Task=6
app-1  | [abc123] [workers-0-C-1] Partition=0 Offset=1 | Received Task=12
app-1  | [abc123] [workers-1-C-1] Partition=2 Offset=0 | Received Task=3
app-1  | [abc123] [workers-2-C-1] Partition=4 Offset=0 | Received Task=5
```

### 3. Scale to 2 instances (simulate 2 Kubernetes pods)

```bash
docker compose up --scale app=2
```

Expected output — **two instances processing in parallel**:
```
app-1  | [abc123] [workers-0-C-1] Partition=0 Offset=0 | Received Task=6
app-2  | [def456] [workers-0-C-1] Partition=3 Offset=0 | Received Task=4
app-1  | [abc123] [workers-1-C-1] Partition=1 Offset=0 | Received Task=2
app-2  | [def456] [workers-1-C-1] Partition=4 Offset=0 | Received Task=5
app-1  | [abc123] [workers-2-C-1] Partition=2 Offset=0 | Received Task=3
app-2  | [def456] [workers-2-C-1] Partition=5 Offset=0 | Received Task=11
```

Notice:
- `app-1` handles partitions 0, 1, 2
- `app-2` handles partitions 3, 4, 5
- **No coordination code** — Kafka distributed automatically

### 4. Simulate pod failure

```bash
# Kill one instance
docker compose stop app-1

# Watch logs — app-2 takes over ALL partitions
docker compose logs -f app-2
```

Expected: Kafka detects the lost consumer, triggers rebalance, and assigns all 6 partitions to `app-2`.

### 5. Recover the failed pod

```bash
docker compose start app-1
```

Expected: Kafka triggers another rebalance and redistributes partitions evenly.

### 6. Check processing status

```bash
docker compose exec app-1 curl -s localhost:8080/api/tasks/status | python -m json.tool
```

Response:
```json
{
    "instance": "abc123def456",
    "pending": 0,
    "processing": 5,
    "done": 95
}
```

### 7. Kafka UI

Open http://localhost:8090 to see:
- Topic `task-processing` with 6 partitions
- Consumer group `workers` with partition assignments
- Message flow and offsets

---

## Project Structure

```
src/main/java/com/example/kafkaworkers/
├── KafkaWorkersApplication.java    # Entry point + @EnableScheduling
├── config/
│   └── KafkaTopicConfig.java       # Topic "task-processing" with 6 partitions
├── entity/
│   ├── Task.java                   # JPA entity (id, description, status)
│   └── TaskStatus.java             # Enum: PENDING, PROCESSING, DONE
├── repository/
│   └── TaskRepository.java         # JPA repository with CAS query
├── producer/
│   └── TaskProducer.java           # KafkaTemplate.send(topic, key, value)
├── consumer/
│   └── TaskConsumer.java           # @KafkaListener — processes tasks
├── scheduler/
│   └── TaskScheduler.java          # @Scheduled — publishes pending tasks
├── service/
│   ├── TaskService.java            # Business logic
│   └── TaskInitializer.java        # Inserts 100 fake tasks on startup
└── controller/
    └── TaskController.java         # GET /api/tasks/status
```

---

## Failure Handling

### Consumer Failure (pod crash)

```
1. Consumer stops sending heartbeats
2. After session.timeout.ms (default 45s), Kafka marks consumer as dead
3. Rebalance triggered
4. Dead consumer's partitions reassigned to surviving consumers
5. New owners resume from last committed offset
6. Messages after the last commit are reprocessed (at-least-once)
```

### Producer Failure (scheduler crash)

```
1. Tasks remain in PENDING status in the database
2. When any instance's scheduler runs, it picks up PENDING tasks
3. CAS query prevents duplicate publishing
4. Processing continues normally
```

### Offset Retry Behavior

```
Message M arrives at offset 5 in partition 0
Consumer processes M → SUCCESS → commits offset 6
Consumer receives next message at offset 6

Message N arrives at offset 6
Consumer processes N → CRASH before commit
Rebalance → new consumer gets partition 0
New consumer polls from offset 6 (last committed)
Message N is redelivered → processed again (at-least-once)
```

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `spring.kafka.consumer.group-id` | `workers` | Consumer group name |
| `spring.kafka.listener.concurrency` | `3` | Consumer threads per instance |
| `spring.kafka.listener.ack-mode` | `record` | Commit after each message |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | Start from beginning if no committed offset |
| `spring.kafka.consumer.enable-auto-commit` | `false` | Application-controlled commits |
| `app.instance-id` | `${HOSTNAME}` | Instance identifier for logs |

---

## Troubleshooting

### Consumers not receiving messages

1. Check Kafka is healthy: `docker compose logs kafka`
2. Check topic exists: open Kafka UI at http://localhost:8090
3. Verify consumer group in Kafka UI → Consumer Groups → `workers`

### Duplicate processing

Expected with at-least-once delivery. If a consumer crashes before committing offset, the message is redelivered. Make processing **idempotent** (the TaskService checks status before processing).

### Rebalance takes too long

Default `session.timeout.ms` is 45 seconds. For faster detection:

```yaml
spring:
  kafka:
    consumer:
      properties:
        session.timeout.ms: 10000
        heartbeat.interval.ms: 3000
```

### All messages go to the same partition

The partition is determined by `hash(key) % numPartitions`. If all keys hash to the same bucket, distribution is uneven. With 100 sequential IDs and 6 partitions, distribution is roughly even.

---

## Key Takeaways

```
✓ Kafka distributes partitions between consumers automatically
✓ Consumers process messages continuously in parallel
✓ No manual pod coordination exists
✓ No database locking coordination exists
✓ Kafka handles distributed coordination
✓ Scaling = adding more consumers (pods or threads)
✓ Failure recovery = automatic partition reassignment
✓ Ordering guaranteed within each partition
```
