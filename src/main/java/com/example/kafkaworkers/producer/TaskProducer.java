package com.example.kafkaworkers.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TaskProducer {

    private static final Logger log = LoggerFactory.getLogger(TaskProducer.class);

    private static final String TOPIC = "task-processing";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TaskProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a task ID to Kafka.
     * <p>
     * Key = taskId (determines target partition via hash).
     * Value = taskId (payload for consumer).
     * <p>
     * Using the task ID as the key ensures that the same task always goes to
     * the same partition, preserving ordering per task.
     */
    public void send(Long taskId) {
        String key = String.valueOf(taskId);
        kafkaTemplate.send(TOPIC, key, key)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send Task={} to Kafka: {}", taskId, ex.getMessage());
                    } else {
                        log.debug("Task={} sent to partition={} offset={}",
                                taskId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
