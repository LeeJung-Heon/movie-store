package com.lecture.booking.kafka;

import com.lecture.booking.service.BookingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class BookingKafkaConsumerTest {

    private BookingService bookingService;
    private BookingKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        bookingService = mock(BookingService.class);
        consumer = new BookingKafkaConsumer(bookingService);
    }

    @Test
    void bookingId를_추출해_예매를_확정한다() {
        Map<String, Object> event = new HashMap<>();
        event.put("paymentId", 5);
        event.put("bookingId", 100);
        event.put("userId", 1);
        event.put("movieId", 7);
        event.put("status", "COMPLETED");

        consumer.handlePaymentCompleted(event);

        verify(bookingService).confirmBooking(100L);
    }

    @Test
    void bookingId가_문자열로_와도_확정한다() {
        Map<String, Object> event = new HashMap<>();
        event.put("bookingId", "100");

        consumer.handlePaymentCompleted(event);

        verify(bookingService).confirmBooking(100L);
    }

    @Test
    void bookingId가_없으면_확정하지_않는다() {
        Map<String, Object> event = new HashMap<>();
        event.put("userId", 1);
        event.put("movieId", 7);

        consumer.handlePaymentCompleted(event);

        verify(bookingService, never()).confirmBooking(any());
    }

    @Test
    void bookingId_형식이_잘못되면_확정하지_않는다() {
        Map<String, Object> event = new HashMap<>();
        event.put("bookingId", "abc");

        consumer.handlePaymentCompleted(event); // 재시도해도 성공할 수 없는 이벤트라 예외 없이 건너뛴다

        verify(bookingService, never()).confirmBooking(any());
    }

    @Test
    void 확정_중_예외는_삼키지_않고_에러_핸들러로_넘긴다() {
        // [부하 실험 수정] 예외를 삼키면 오프셋이 커밋되어 확정 이벤트가 사라진다(동시 20건 실험에서 6건 유실).
        // 밖으로 던져야 DefaultErrorHandler가 재시도하고, 끝까지 실패하면 DLT로 보낸다.
        Map<String, Object> event = new HashMap<>();
        event.put("bookingId", 999);
        doThrow(new IllegalArgumentException("없음")).when(bookingService).confirmBooking(999L);

        assertThrows(IllegalArgumentException.class, () -> consumer.handlePaymentCompleted(event));

        verify(bookingService).confirmBooking(999L);
    }
}
