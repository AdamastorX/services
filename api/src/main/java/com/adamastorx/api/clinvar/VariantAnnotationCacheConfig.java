package com.adamastorx.api.clinvar;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Same shape as {@code workitem.WorkItemCacheConfig} (services#15, ADR
 * 0016) -- a hand-built, typed {@code RedisTemplate} rather than Boot's
 * untyped JDK-serialization default, same reasoning: portability, smaller
 * payloads, human-readable from {@code redis-cli}.
 */
@Configuration
public class VariantAnnotationCacheConfig {

    @Bean
    public RedisTemplate<String, VariantAnnotation> variantAnnotationRedisTemplate(
            RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, VariantAnnotation> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new JacksonJsonRedisSerializer<>(VariantAnnotation.class));
        template.afterPropertiesSet();
        return template;
    }
}
