package com.example.kafkaworkers.controller;

import com.example.kafkaworkers.entity.TaskStatus;
import com.example.kafkaworkers.service.TaskService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskService taskService;
    private final String instanceId;

    public TaskController(TaskService taskService,
                          @Value("${app.instance-id}") String instanceId) {
        this.taskService = taskService;
        this.instanceId = instanceId;
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("instance", instanceId);
        status.put("pending", taskService.countByStatus(TaskStatus.PENDING));
        status.put("processing", taskService.countByStatus(TaskStatus.PROCESSING));
        status.put("done", taskService.countByStatus(TaskStatus.DONE));
        return status;
    }
}
