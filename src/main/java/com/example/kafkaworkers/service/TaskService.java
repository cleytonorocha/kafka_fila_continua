package com.example.kafkaworkers.service;

import com.example.kafkaworkers.entity.Task;
import com.example.kafkaworkers.entity.TaskStatus;
import com.example.kafkaworkers.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    private final TaskRepository taskRepository;

    public TaskService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    /**
     * Atomic compare-and-swap: PENDING → PROCESSING.
     * Returns true if THIS instance successfully claimed the task.
     * Returns false if another instance already claimed it.
     */
    @Transactional
    public boolean markAsProcessing(Long taskId) {
        int updated = taskRepository.compareAndSwapStatus(
                taskId, TaskStatus.PROCESSING, TaskStatus.PENDING);
        return updated > 0;
    }

    /**
     * Processes a task: loads from DB, simulates heavy work, marks as DONE.
     *
     * This method is called by the Kafka consumer after receiving a task ID.
     * The consumer has exclusive ownership of its assigned partitions,
     * so no two consumers will process the same message concurrently.
     */
    @Transactional
    public void processTask(Long taskId) {
        Optional<Task> optionalTask = taskRepository.findById(taskId);

        if (optionalTask.isEmpty()) {
            log.warn("Task {} not found in database — skipping", taskId);
            return;
        }

        Task task = optionalTask.get();

        if (task.getStatus() == TaskStatus.DONE) {
            log.warn("Task {} already DONE — skipping (idempotent)", taskId);
            return;
        }

        // Simulate heavy processing (1–3 seconds)
        simulateHeavyProcessing(taskId);

        task.setStatus(TaskStatus.DONE);
        taskRepository.save(task);
    }

    public List<Task> findPendingTasks() {
        return taskRepository.findByStatusOrderByIdAsc(TaskStatus.PENDING);
    }

    public long countByStatus(TaskStatus status) {
        return taskRepository.countByStatus(status);
    }

    private void simulateHeavyProcessing(Long taskId) {
        try {
            long processingTimeMs = ThreadLocalRandom.current().nextLong(1000, 3001);
            log.debug("Task={} simulating {}ms of heavy processing...", taskId, processingTimeMs);
            Thread.sleep(processingTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Processing interrupted for Task={}", taskId);
        }
    }
}
