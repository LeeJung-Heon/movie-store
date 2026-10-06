package com.lecture.booking.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

/**
 * [부하 실험 수정] payment.completed 처리 실패 시 1초 간격으로 최대 10회 재시도하고,
 * 그래도 실패하면 payment.completed.DLT 로 격리한다. (조용한 유실 방지)
 */
@Slf4j
@Configuration
public class KafkaErrorHandlingConfig {

    private static final String DLT_SUFFIX = ".DLT";

    @Value("${kafka.topic.payment-completed}")
    private String paymentCompletedTopic;

    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> template) {
        // 기본 DLT 이름은 spring-kafka 버전마다 다르다(3.3.5는 '-dlt'). 아래 토픽 생성과 이름을 맞추려고 직접 지정한다.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 10L));
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("[Kafka] {}-{}@{} 처리 실패 {}회째: {}", record.topic(), record.partition(), record.offset(),
                        deliveryAttempt, NestedExceptionUtils.getMostSpecificCause(ex).getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("[Kafka] {}-{}@{} 재시도 소진, {}{}로 격리", record.topic(), record.partition(), record.offset(),
                        record.topic(), DLT_SUFFIX);
            }
        });
        return handler;
    }

    /** DLT는 원본과 같은 파티션 번호로 보내므로 원본(3개)과 같은 파티션 수로 만든다. */
    @Bean
    public NewTopic paymentCompletedDltTopic() {
        return TopicBuilder.name(paymentCompletedTopic + DLT_SUFFIX)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
