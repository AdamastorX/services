package com.adamastorx.api.workitem;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface WorkItemJpaRepository extends JpaRepository<WorkItemEntity, UUID> {
}
