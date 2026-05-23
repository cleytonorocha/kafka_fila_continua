package com.example.kafkaworkers.repository;

import com.example.kafkaworkers.entity.Task;
import com.example.kafkaworkers.entity.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TaskRepository extends JpaRepository<Task, Long> {

    List<Task> findByStatusOrderByIdAsc(TaskStatus status);

    /**
     * Atomic compare-and-swap: only updates if current status matches expectedStatus.
     * Returns 1 if updated, 0 if another instance already claimed this task.
     */
    @Modifying
    @Query("UPDATE Task t SET t.status = :newStatus WHERE t.id = :id AND t.status = :expectedStatus")
    int compareAndSwapStatus(@Param("id") Long id,
                             @Param("newStatus") TaskStatus newStatus,
                             @Param("expectedStatus") TaskStatus expectedStatus);

    long countByStatus(TaskStatus status);
}
