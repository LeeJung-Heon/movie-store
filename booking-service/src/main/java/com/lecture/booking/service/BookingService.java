package com.lecture.booking.service;

import com.lecture.booking.dto.BookingDto;
import com.lecture.booking.entity.Booking;
import com.lecture.booking.kafka.BookingKafkaProducer;
import com.lecture.booking.kafka.KafkaEvent;
import com.lecture.booking.repository.BookingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class BookingService {

    private final BookingRepository bookingRepository;
    private final MovieServiceClient movieServiceClient;
    private final PaymentServiceClient paymentServiceClient;
    private final BookingKafkaProducer kafkaProducer;
    private final BookingWriteService bookingWriteService;

    /**
     * 예매 흐름
     * 1. 영화 존재 확인 (동기)
     * 2. 티켓 단가 조회 후 금액 계산
     * 3. PENDING 예매 생성 및 즉시 커밋
     * 4. 결제 요청 (동기) - bookingId 전달
     *
     * 중복 예매 검사는 하지 않는다. 같은 영화를 여러 번 예매할 수 있다.
     */
    // [부하 실험 수정] 원격 호출(영화 조회·결제) 동안 DB 커넥션을 쥐지 않도록 바깥 트랜잭션을 두지 않는다.
    // 바깥 readOnly 트랜잭션이 커넥션을 잡은 채 REQUIRES_NEW 가 커넥션을 하나 더 요구하면,
    // 동시 요청이 풀 크기(10)를 넘을 때 서로를 기다리며 30초 타임아웃으로 실패했다.
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public BookingDto.BookingResponse book(Long userId, BookingDto.BookRequest request) {
        Long movieId = request.getMovieId();
        int quantity = request.getQuantity() == null ? 1 : request.getQuantity();

        if (!movieServiceClient.existsMovie(movieId)) {
            throw new IllegalArgumentException("존재하지 않는 영화입니다: " + movieId);
        }

        BigDecimal amount = movieServiceClient.getPrice(movieId)
                .multiply(BigDecimal.valueOf(quantity));

        Booking booking = bookingWriteService.createPendingBooking(userId, movieId, quantity, amount);

        PaymentServiceClient.PaymentResult result;
        try {
            result = paymentServiceClient.requestPayment(userId, booking.getId(), movieId, amount);
        } catch (RuntimeException e) {
            // createPendingBooking 이 REQUIRES_NEW 로 이미 커밋한 행이므로,
            // 결제 호출 자체가 실패해도 예매를 되돌려 PENDING 고아 행이 남지 않게 한다.
            log.error("[BookingService] 결제 요청 실패 - bookingId: {}, error: {}",
                    booking.getId(), e.getMessage());
            bookingWriteService.cancelBooking(booking.getId());
            throw e;
        }

        if (result == null || !"COMPLETED".equals(result.getStatus())) {
            log.warn("[BookingService] 결제 실패 - bookingId: {}, status: {}",
                    booking.getId(), result != null ? result.getStatus() : "null");
            bookingWriteService.cancelBooking(booking.getId());
            throw new IllegalStateException("결제에 실패했습니다");
        }

        log.info("[BookingService] 예매 접수 (결제 대기) - bookingId: {}", booking.getId());
        return BookingDto.BookingResponse.from(booking);
    }

    /**
     * 예매 확정 - payment.completed 수신 시 호출된다.
     * bookingId 로 특정하므로 같은 영화의 다른 예매 건에 영향을 주지 않는다.
     */
    @Transactional
    public void confirmBooking(Long bookingId) {
        Booking booking = bookingRepository.findById(bookingId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "예매 정보를 찾을 수 없습니다 - bookingId: " + bookingId));

        if (booking.getStatus() == Booking.Status.CONFIRMED) {
            log.info("[BookingService] 이미 확정된 예매 - 중복 이벤트 무시: {}", bookingId);
            return;
        }

        booking.confirm();

        movieServiceClient.increaseBookingCount(booking.getMovieId());

        String genre = resolveGenre(booking.getMovieId());

        kafkaProducer.publishBookingCompleted(
                KafkaEvent.BookingCompletedEvent.builder()
                        .bookingId(booking.getId())
                        .userId(booking.getUserId())
                        .movieId(booking.getMovieId())
                        .genre(genre)
                        .build()
        );

        log.info("[BookingService] 예매 확정 완료 - bookingId: {}", booking.getId());
    }

    /** 사용자 예매 목록 - movie-service에서 영화 정보를 붙여 반환 */
    public List<BookingDto.BookingResponse> getBookingsByUser(Long userId) {
        return bookingRepository.findByUserIdOrderByCreatedAtDesc(userId).stream()
                .map(booking -> BookingDto.BookingResponse.from(
                        booking, toMovieSummary(booking.getMovieId())))
                .collect(Collectors.toList());
    }

    /** 추천 서비스용: 확정된 예매의 영화 ID 목록 */
    public BookingDto.BookingHistoryResponse getBookingHistory(Long userId) {
        List<Long> movieIds = bookingRepository
                .findByUserIdAndStatus(userId, Booking.Status.CONFIRMED)
                .stream()
                .map(Booking::getMovieId)
                .distinct()
                .collect(Collectors.toList());

        return BookingDto.BookingHistoryResponse.builder()
                .userId(userId)
                .movieIds(movieIds)
                .build();
    }

    private String resolveGenre(Long movieId) {
        try {
            Object genre = movieServiceClient.getMovie(movieId).get("genre");
            return genre != null ? genre.toString() : null;
        } catch (Exception e) {
            log.warn("[BookingService] 장르 조회 실패 - movieId: {}, error: {}", movieId, e.getMessage());
            return null;
        }
    }

    private BookingDto.MovieSummary toMovieSummary(Long movieId) {
        try {
            Map<String, Object> movie = movieServiceClient.getMovie(movieId);
            return BookingDto.MovieSummary.builder()
                    .id(toLong(movie.get("id")))
                    .title(toStringOrNull(movie.get("title")))
                    .genre(toStringOrNull(movie.get("genre")))
                    .posterUrl(toStringOrNull(movie.get("posterUrl")))
                    .price(toBigDecimal(movie.get("price")))
                    .openDt(toStringOrNull(movie.get("openDt")))
                    .build();
        } catch (Exception e) {
            log.warn("[BookingService] 영화 요약 조회 실패 - movieId: {}, error: {}",
                    movieId, e.getMessage());
            return null;
        }
    }

    private Long toLong(Object value) {
        return value instanceof Number number ? number.longValue() : null;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof Number number) {
            return BigDecimal.valueOf(number.doubleValue());
        }
        return null;
    }

    private String toStringOrNull(Object value) {
        return value == null ? null : value.toString();
    }
}
