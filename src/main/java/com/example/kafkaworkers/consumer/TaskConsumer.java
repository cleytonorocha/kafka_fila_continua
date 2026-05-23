package com.example.kafkaworkers.consumer;

import com.example.kafkaworkers.service.TaskService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that processes tasks from the "task-processing" topic.
 *
 * KEY CONCEPTS:
 *
 * 1. CONSUMER GROUP ("workers"):
 *    All consumers with the same groupId form a consumer group.
 *    Kafka distributes partitions among consumers in the SAME group.
 *    Each partition is assigned to exactly ONE consumer in the group.
 *
 * 2. CONTINUOUS PROCESSING LOOP:
 *    Each consumer thread continuously executes:
 *      poll() → process() → commit offset → poll() → ...
 *    The consumer never stops. It continuously polls for new messages.
 *
 * 3. PARTITION OWNERSHIP:
 *    Once a partition is assigned to a consumer, that consumer is the
 *    EXCLUSIVE owner of that partition. No other consumer in the same
 *    group will receive messages from that partition.
 *
 * 4. REBALANCE:
 *    When a consumer joins or leaves the group, Kafka triggers a rebalance.
 *    Partitions are redistributed among the remaining consumers.
 *    This happens automatically — no application code required.
 *
 * 5. OFFSET:
 *    Each message in a partition has a sequential offset number.
 *    After processing a message, the consumer commits the offset.
 *    If a consumer crashes, the new owner of that partition resumes
 *    from the last committed offset (at-least-once delivery).
 */
@Component
public class TaskConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskConsumer.class);

    private final TaskService taskService;
    private final String instanceId;

    public TaskConsumer(TaskService taskService,
                        @Value("${app.instance-id}") String instanceId) {
        this.taskService = taskService;
        this.instanceId = instanceId;
    }

    /**
     * Kafka listener — the entry point for message consumption.
     *
     * Spring Kafka creates N consumer threads based on the "concurrency" setting.
     * Each thread independently polls its assigned partitions.
     *
     * With concurrency=3 and 6 partitions:
     *   - Thread-0 → partitions [0, 1]
     *   - Thread-1 → partitions [2, 3]
     *   - Thread-2 → partitions [4, 5]
     *
     * With 2 instances (concurrency=3 each) = 6 consumers total:
     *   - Instance-1/Thread-0 → partition [0]
     *   - Instance-1/Thread-1 → partition [1]
     *   - Instance-1/Thread-2 → partition [2]
     *   - Instance-2/Thread-0 → partition [3]
     *   - Instance-2/Thread-1 → partition [4]
     *   - Instance-2/Thread-2 → partition [5]
     */
    @KafkaListener(topics = "task-processing", groupId = "workers")
    public void consume(ConsumerRecord<String, String> message) {
        Long taskId = Long.valueOf(message.value());

        log.info("[{}] [{}] Partition={} Offset={} | Received Task={}",
                instanceId,
                Thread.currentThread().getName(),
                message.partition(),
                message.offset(),
                taskId);

        taskService.processTask(taskId);

        log.info("[{}] [{}] Partition={} | Task={} DONE",
                instanceId,
                Thread.currentThread().getName(),
                message.partition(),
                taskId);
    }
}
