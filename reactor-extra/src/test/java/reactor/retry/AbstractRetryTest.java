package reactor.retry;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractRetryTest {

    @Test
    public void calculateTimeoutUsesScheduler() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
                Backoff.ZERO_BACKOFF, Jitter.NO_JITTER, scheduler, null) {
            @Override
            public Publisher<Long> apply(Flux<Integer> integerFlux) {
                return null;
            }
        };

        assertThat(abstractRetry.calculateTimeout().toEpochMilli())
                .as("at clock 0")
                .isEqualTo(1000L);

        scheduler.advanceTimeBy(Duration.ofSeconds(3));

        assertThat(abstractRetry.calculateTimeout().toEpochMilli())
                .as("after clock move")
                .isEqualTo(4000L);
    }

    @Test
    public void calculateBackoffUsesScheduler() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        Instant timeoutInstant = Instant.ofEpochMilli(1000);

        AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
                Backoff.fixed(Duration.ofMillis(600)), Jitter.NO_JITTER, scheduler, null) {
            @Override
            public Publisher<Long> apply(Flux<Integer> integerFlux) {
                return null;
            }
        };

        RetryContext<String> retryContext = new DefaultContext<>(null, 1, BackoffDelay.ZERO, null);

        BackoffDelay backoff = abstractRetry.calculateBackoff(retryContext, timeoutInstant);
        assertThat(backoff)
                .as("at clock 0")
                .isNotSameAs(AbstractRetry.RETRY_EXHAUSTED)
                .satisfies(b -> assertThat(b.delay).isEqualTo(Duration.ofMillis(600)));

        scheduler.advanceTimeBy(Duration.ofMillis(500));

        backoff = abstractRetry.calculateBackoff(retryContext, timeoutInstant);
        assertThat(backoff)
                .as("at clock 500")
                .isSameAs(AbstractRetry.RETRY_EXHAUSTED);
    }

    @Test
    public void calculateTimeoutUsesDefaultClockWhenNoScheduler() {
        AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
                Backoff.ZERO_BACKOFF, Jitter.NO_JITTER, null, null) {
            @Override
            public Publisher<Long> apply(Flux<Integer> integerFlux) {
                return null;
            }
        };

        assertThat(abstractRetry.calculateTimeout().toEpochMilli())
                .isGreaterThanOrEqualTo(1000L + Instant.now().toEpochMilli());
    }

    @Test
    public void calculateBackoffUsesDefaultClockWhenNoScheduler() {
        Instant timeoutInstant = Instant.now().plusSeconds(3);

        AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
                Backoff.fixed(Duration.ofMillis(600)), Jitter.NO_JITTER, null, null) {
            @Override
            public Publisher<Long> apply(Flux<Integer> integerFlux) {
                return null;
            }
        };

        RetryContext<String> retryContext = new DefaultContext<>(null, 1, BackoffDelay.ZERO, null);

        BackoffDelay backoff = abstractRetry.calculateBackoff(retryContext, timeoutInstant);
        assertThat(backoff)
                .isNotSameAs(AbstractRetry.RETRY_EXHAUSTED)
                .satisfies(b -> assertThat(b.delay).isEqualTo(Duration.ofMillis(600)));
    }

}