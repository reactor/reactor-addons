package reactor.extra;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

public class SwitchTransformOnFirstFluxTest {


    @Test
    public void shouldRequestExpectedAmountOfElements() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong capture = new AtomicLong();
        AtomicLong requested = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::addAndGet)
                                                .transform(flux -> new SwitchTransformOnFirstFlux<>(
                                                        flux,
                                                        (first, innerFlux) -> innerFlux));

        publisher.next(1L);

        switchTransformed.subscribe(capture::set, __ -> {}, latch::countDown,
                s -> {
                    for (int i = 0; i < 10000; i++) {
                        RaceTestUtils.race(() -> s.request(1), () -> s.request(1));
                    }
                    RaceTestUtils.race(publisher::complete, publisher::complete);
                });

        latch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(capture.get(), 1L);
        Assert.assertEquals(requested.get(), 20000L);
    }

    @Test
    public void shouldReturnCorrectContextOnEmptySource() {
        Flux<Long> switchTransformed = Flux
                .<Long>empty()
                .transform(flux -> new SwitchTransformOnFirstFlux<>(
                        flux,
                        (first, innerFlux) -> innerFlux
                ))
                .subscriberContext(Context.of("a", "c"))
                .subscriberContext(Context.of("c", "d"));

        StepVerifier.create(switchTransformed, 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .expectAccessibleContext()
                    .contains("a", "c")
                    .contains("c", "d")
                    .then()
                    .expectComplete()
                    .verify();
    }

    @Test
    public void shouldNotFailOnIncorrectPublisherBehavior() {
        TestPublisher<Long> publisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
        Flux<Long> switchTransformed = publisher
                .flux()
                .transform(flux -> new SwitchTransformOnFirstFlux<>(
                    flux,
                    (first, innerFlux) -> innerFlux
                            .subscriberContext(Context.of("a", "b"))
                ));

        StepVerifier.create(new Flux<Long>() {
            @Override
            public void subscribe(CoreSubscriber<? super Long> actual) {
                switchTransformed.subscribe(actual);
                publisher.next(1L);
            }
        }, 0)
                    .thenRequest(1)
                    .expectNext(1L)
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext(2L)
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .expectError()
                    .verifyThenAssertThat()
                    .hasDroppedErrors(3)
                    .tookLessThan(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

//    @Test
//    public void shouldNotFailOnIncorrePu

    @Test
    public void shouldBeAbleToAccessUpstreamContext() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher
                .flux()
                .transform(flux -> new SwitchTransformOnFirstFlux<>(
                        flux,
                        (first, innerFlux) -> innerFlux.map(String::valueOf)
                                                       .subscriberContext(Context.of("a", "b"))
                ))
                .subscriberContext(Context.of("a", "c"))
                .subscriberContext(Context.of("c", "d"));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .expectAccessibleContext()
                        .contains("a", "b")
                        .contains("c", "d")
                    .then()
                    .then(publisher::complete)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldNotHangWhenOneElementUpstream() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher
                .flux()
                .transform(flux -> new SwitchTransformOnFirstFlux<>(
                        flux,
                        (first, innerFlux) -> innerFlux.map(String::valueOf)
                                                       .subscriberContext(Context.of("a", "b"))
                ))
                .subscriberContext(Context.of("a", "c"))
                .subscriberContext(Context.of("c", "d"));

        publisher.next(1L);
        publisher.complete();

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void backpressureTest() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .transform(flux -> new SwitchTransformOnFirstFlux<>(
                                                          flux,
                                                          (first, innerFlux) -> innerFlux.map(String::valueOf)));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .then(publisher::complete)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldErrorOnOverflowTest() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .transform(flux -> new SwitchTransformOnFirstFlux<>(
                                                          flux,
                                                          (first, innerFlux) -> innerFlux.map(
                                                                  String::valueOf)));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .then(() -> publisher.next(2L))
                    .expectError()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldPropagateonCompleteCorrectly() {
        Flux<String> switchTransformed = Flux.empty()
                                             .transform(flux -> new SwitchTransformOnFirstFlux<>(
                                                     flux,
                                                     (first, innerFlux) -> innerFlux.map(
                                                             String::valueOf)));

        StepVerifier.create(switchTransformed)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldPropagateErrorCorrectly() {
        Flux<String> switchTransformed = Flux.error(new RuntimeException("hello"))
                                             .transform(flux -> new SwitchTransformOnFirstFlux<>(
                                                     flux,
                                                     (first, innerFlux) -> innerFlux.map(
                                                             String::valueOf)));

        StepVerifier.create(switchTransformed)
                    .expectErrorMessage("hello")
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                                                  .transform(flux -> new SwitchTransformOnFirstFlux<>(
                                                          flux,
                                                          (first, innerFlux) -> innerFlux.map(
                                                                  String::valueOf)));

        publisher.next(1);

        StepVerifier.create(switchTransformed, 0)
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();
    }
}