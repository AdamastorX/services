package com.adamastorx.workers;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling} activates {@code ClinVarIngestionScheduler}'s
 * {@code @Scheduled} weekly trigger (services#25, ADR 0018) -- without
 * it, {@code @Scheduled} methods are silently never invoked (no error, no
 * log line), same class of gotcha as this project's other "the
 * annotation alone isn't enough" findings (see
 * {@code adamastorx/docs/SESSION_STATE.md}).
 */
@SpringBootApplication
@EnableScheduling
public class WorkersApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkersApplication.class, args);
    }
}
