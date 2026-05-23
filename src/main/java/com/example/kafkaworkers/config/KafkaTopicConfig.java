package com.example.kafkaworkers.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    /**
     * Creates the "task-processing" topic with 6 partitions.
     *
     * WHY 6 PARTITIONS?
     * - Partitions are the unit of parallelism in Kafka
     * - Each partition can have at most ONE active consumer per consumer group
     * - 6 partitions = maximum 6 parallel consumers in one group
     * - With concurrency=3 per instance, 2 instances = 6 consumers = perfect distribution
     */
    @Bean
    public NewTopic taskProcessingTopic() {
        return TopicBuilder.name("task-processing")
                .partitions(6)
                .replicas(1)
                .build();
    }
}
