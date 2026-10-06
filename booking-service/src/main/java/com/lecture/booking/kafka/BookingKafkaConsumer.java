package com.lecture.booking.kafka;

import com.lecture.booking.service.BookingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class BookingKafkaConsumer {

    private final BookingService bookingService;

    /**
     * payment.completed 수신 → 예매 PENDING → CONFIRMED
     *
     * payment-service는 JsonSerializer에 type header를 싣지 않으므로
     * 특정 DTO가 아니라 Map으로 받아 직접 파싱한다.
     */
    @KafkaListener(
            topics = "${kafka.topic.payment-completed}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handlePaymentCompleted(Map<String, Object> event) {
        log.info("[Kafka Consumer] payment.completed 수신: {}", event);

        Object bookingIdValue = event.get("bookingId");
        Long bookingId;
        try {
            if (bookingIdValue == null) {
                throw new IllegalArgumentException(
                        "payment.completed 이벤트에 bookingId가 없습니다. payment-service 계약을 확인하세요.");
            }
            bookingId = bookingIdValue instanceof Number number
                    ? number.longValue()
                    : Long.parseLong(bookingIdValue.toString().trim());
        } catch (IllegalArgumentException e) {
            // 계약 위반 이벤트는 재시도해도 성공하지 않으므로 기록만 하고 넘긴다
            log.error("[Kafka Consumer] 잘못된 이벤트 - event: {}, error: {}", event, e.getMessage());
            return;
        }

        // [부하 실험 수정] DB 커넥션 부족 같은 일시 오류를 삼키면 오프셋이 커밋되어
        // '결제 완료 + 예매 PENDING' 불일치가 영구히 남는다. 예외를 던져 에러 핸들러가 재시도하게 한다.
        bookingService.confirmBooking(bookingId);
        log.info("[Kafka Consumer] 예매 확정 완료 - bookingId: {}", bookingId);
    }
}
