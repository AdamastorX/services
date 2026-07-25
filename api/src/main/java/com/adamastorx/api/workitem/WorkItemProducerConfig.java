package com.adamastorx.api.workitem;

import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Spring Boot auto-configures a {@code KafkaTemplate<Object, Object>} from
 * {@code spring.kafka.producer.*} (application.yml). That's untyped by
 * design (it has to serve any module), so {@link WorkItemProducer} gets
 * its own typed {@code KafkaTemplate<String, WorkItem>} here, built from
 * the same {@link KafkaProperties} -- same serializer/config, just a
 * concrete generic type instead of {@code Object}/{@code Object}.
 */
@Configuration
public class WorkItemProducerConfig {

    @Bean
    public ProducerFactory<String, WorkItem> workItemProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, WorkItem> workItemKafkaTemplate(
            ProducerFactory<String, WorkItem> workItemProducerFactory) {
        KafkaTemplate<String, WorkItem> template = new KafkaTemplate<>(workItemProducerFactory);
        // Boot's spring.kafka.template.observation-enabled only wires
        // observation into Boot's own auto-configured KafkaTemplate --
        // this bean is hand-built (see class javadoc) and bypasses that
        // autoconfiguration entirely, so it needs enabling directly
        // (observability#1, ADR 0013). Without this, the send() call
        // creates no span and doesn't propagate the caller's trace
        // context into the record's headers -- found by checking
        // workers' consumer logs and seeing an empty trace-id bracket
        // despite api's own request-handling span being correctly
        // populated, not assumed from the framework's docs.
        template.setObservationEnabled(true);
        return template;
    }
}
