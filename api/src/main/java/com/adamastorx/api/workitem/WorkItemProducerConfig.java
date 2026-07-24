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
        return new KafkaTemplate<>(workItemProducerFactory);
    }
}
