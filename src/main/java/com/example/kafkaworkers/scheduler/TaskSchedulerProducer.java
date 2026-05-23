package com.example.kafkaworkers.scheduler;

import com.example.kafkaworkers.entity.Task;
import com.example.kafkaworkers.producer.TaskProducer;
import com.example.kafkaworkers.service.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Scheduler that periodically scans the database for PENDING tasks
 * and publishes their IDs to Kafka.
 *
 * IMPORTANT: Multiple instances may run this scheduler simultaneously.
 * The TaskService.markAsProcessing() uses a compare-and-swap (CAS) query:
 *   UPDATE task SET status='PROCESSING' WHERE id=? AND status='PENDING'
 *
 * This guarantees that only ONE instance successfully claims each task.
 * If Instance-A and Instance-B both read Task-42 as PENDING:
 *   - Instance-A executes CAS → returns 1 (success) → publishes to Kafka
 *   - Instance-B executes CAS → returns 0 (already claimed) → skips
 *
 * Result: each task is published to Kafka EXACTLY ONCE.
 * No distributed locks needed. No leader election needed.
 */
@Component
public class TaskSchedulerProducer {

    private static final Logger log = LoggerFactory.getLogger(TaskSchedulerProducer.class);

    private final TaskService taskService;
    private final TaskProducer taskProducer;
    private final String instanceId;

    public TaskSchedulerProducer(TaskService taskService,
                         TaskProducer taskProducer,
                         @Value("${app.instance-id}") String instanceId) {
        this.taskService = taskService;
        this.taskProducer = taskProducer;
        this.instanceId = instanceId;
    }

    @Scheduled(fixedDelay = 5000)
    public void publishPendingTasks() {
        List<Task> pendingTasks = taskService.findPendingTasks();

        if (pendingTasks.isEmpty()) {
            return;
        }

        log.info("[{}] Found {} PENDING tasks", instanceId, pendingTasks.size());

        int published = 0;
        for (Task task : pendingTasks) {
            boolean claimed = taskService.markAsProcessing(task.getId());
            if (claimed) {
                taskProducer.send(task.getId());
                published++;
            }
        }

        log.info("[{}] Published {} tasks to Kafka (skipped {} already claimed)",
                instanceId, published, pendingTasks.size() - published);
    }
}
