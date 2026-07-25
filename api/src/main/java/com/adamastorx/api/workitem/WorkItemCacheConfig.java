package com.adamastorx.api.workitem;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Boot auto-configures a {@code RedisConnectionFactory} (Lettuce, from
 * {@code spring-boot-starter-data-redis}) plus an untyped
 * {@code RedisTemplate<Object, Object>} that defaults to JDK serialization
 * for values -- not portable, bigger payloads, not human-readable from
 * {@code redis-cli}. Same reasoning {@code WorkItemProducerConfig} already
 * used for {@code KafkaTemplate} (ADR 0011): a typed template is hand-built
 * here instead -- {@link StringRedisSerializer} for keys,
 * {@link JacksonJsonRedisSerializer} for values, using the shared Jackson 3
 * {@code JsonMapper} default (no custom {@code ObjectMapper} needed) --
 * the same Jackson stack already on the classpath for this app's REST
 * responses (Boot 4.1's {@code spring-boot-starter-webmvc} default), not a
 * reuse of the classic Jackson 2 {@code jackson-databind} dependency this
 * module also carries, which is scoped specifically to matching
 * spring-kafka's {@code JsonSerializer} wire format (ADR 0011) -- a
 * different, unrelated contract.
 */
@Configuration
public class WorkItemCacheConfig {

    @Bean
    public RedisTemplate<String, WorkItem> workItemRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, WorkItem> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new JacksonJsonRedisSerializer<>(WorkItem.class));
        template.afterPropertiesSet();
        return template;
    }
}
