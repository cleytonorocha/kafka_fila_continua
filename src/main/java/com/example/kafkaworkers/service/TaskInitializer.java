package com.example.kafkaworkers.service;

import com.example.kafkaworkers.entity.Task;
import com.example.kafkaworkers.entity.TaskStatus;
import com.example.kafkaworkers.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Inserts 100 fake tasks on startup if the database is empty.
 *
 * IMPORTANT for multi-instance: if two instances start simultaneously,
 * both may see count=0 and insert 100 tasks each (200 total).
 * For this demo, start 1 instance first, then scale up.
 * In production, use Flyway/Liquibase migrations for seed data.
 */
@Component
public class TaskInitializer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TaskInitializer.class);

    private final TaskRepository taskRepository;

    public TaskInitializer(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @Override
    public void run(String... args) {
        if (taskRepository.count() > 0) {
            log.info("Database already contains {} tasks — skipping initialization",
                    taskRepository.count());
            return;
        }

        log.info("Inserting 100 fake tasks...");
        for (int i = 1; i <= 100; i++) {
            taskRepository.save(new Task(
                    "Task-" + i + ": Process document batch #" + i,
                    TaskStatus.PENDING
            ));
        }
        log.info("100 tasks inserted successfully with status=PENDING");
    }
}
