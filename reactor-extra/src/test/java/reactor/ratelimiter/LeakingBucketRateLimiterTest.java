package reactor.ratelimiter;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import org.assertj.core.data.Offset;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.*;

public class LeakingBucketRateLimiterTest {

	@Test
	public void acquireOrRejectSubscribedErrors() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		assertThatCode(testRateLimiter.acquireOrReject()::block)
				.as("permit 1")
				.doesNotThrowAnyException();
		assertThatCode(testRateLimiter.acquireOrReject()::block)
				.as("permit 2")
				.doesNotThrowAnyException();
		assertThatCode(testRateLimiter.acquireOrReject()::block)
				.as("permit 3")
				.doesNotThrowAnyException();

		assertThatExceptionOfType(RateLimitedException.class)
				.as("permit 4 rejected")
				.isThrownBy(testRateLimiter.acquireOrReject()::block);
	}

	@Test
	public void acquireOrRejectDoNotConsumePermitWithoutSubscribe() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		testRateLimiter.acquireOrReject();
		testRateLimiter.acquireOrReject();
		testRateLimiter.acquireOrReject();

		assertThat(testRateLimiter.acquireOrReject().block()).as("actual permit 1").isNull();
		assertThat(testRateLimiter.acquireOrReject().block()).as("actual permit 2").isNull();
		assertThat(testRateLimiter.acquireOrReject().block()).as("actual permit 3").isNull();
	}

	@Test
	public void acquireOrRejectDoNotConsumePermitWithoutRequest() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		testRateLimiter.acquireOrReject().subscribe(new BaseSubscriber<Void>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {/*NO-OP*/}

			@Override
			protected void hookOnComplete() {
				throw new AssertionError("permit A should not complete");
			}
		});
		testRateLimiter.acquireOrReject().subscribe(new BaseSubscriber<Void>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {/*NO-OP*/}

			@Override
			protected void hookOnComplete() {
				throw new AssertionError("permit B should not complete");
			}
		});
		testRateLimiter.acquireOrReject().subscribe(new BaseSubscriber<Void>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {/*NO-OP*/}

			@Override
			protected void hookOnComplete() {
				throw new AssertionError("permit C should not complete");
			}
		});

		assertThat(testRateLimiter.acquireOrReject().block()).as("actual permit 1").isNull();
		assertThat(testRateLimiter.acquireOrReject().block()).as("actual permit 2").isNull();
		assertThat(testRateLimiter.acquireOrReject().block()).as("actual permit 3").isNull();
	}

	@Test
	public void acquireOrRejectOnDisposedRateLimiter() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		testRateLimiter.dispose();

		StepVerifier.create(testRateLimiter.acquireOrReject())
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("The RateLimiter has been disposed"))
		            .verify();
	}

	@Test
	public void acquireOrWaitOnDisposedRateLimiter() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		testRateLimiter.dispose();

		StepVerifier.create(testRateLimiter.acquireOrWait())
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("The RateLimiter has been disposed"))
		            .verify();
	}

	@Test
	public void tryAcquireOnDisposedRateLimiter() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		testRateLimiter.dispose();

		StepVerifier.create(testRateLimiter.tryAcquire())
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("The RateLimiter has been disposed"))
		            .verify();
	}


	@Test
	public void acquireWaitsForPermit() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));
		AtomicLong timer = new AtomicLong();

		StepVerifier.create(Flux.merge(
				testRateLimiter.acquireOrWait().thenReturn("A"),
				testRateLimiter.acquireOrWait().thenReturn("B"),
				testRateLimiter.acquireOrWait().thenReturn("C"),
				testRateLimiter.acquireOrWait().thenReturn("D")
				               .doOnNext(n -> timer.getAndAccumulate(System.currentTimeMillis(), (start, end) -> end - start))
				).log())
		            .expectSubscription()
		            .then(() -> timer.set(System.currentTimeMillis()))
		            .expectNext("A", "B", "C")
		            .expectNoEvent(Duration.ofMillis(600))
		            .expectNext("D")
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		assertThat(timer.get()).as("D had to wait")
		                       .isCloseTo(1000, Offset.offset(100L));
	}

	@Test
	public void delayWithAcquireWaitUnderRate() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		Flux<Long> source =
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(500))
				    .elapsed()
				    .delayUntil(i -> testRateLimiter.acquireOrWait())
				    .elapsed()
				    .map(delays -> delays.getT1() - delays.getT2().getT1());

		//source represents the elapsed difference between before and after the acquireOrWait
		StepVerifier.create(source)
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(1) doesn't introduce significant wait").isLessThan(1))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(2) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(3) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(4) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(5) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(6) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(7) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(8) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(9) doesn't introduce significant wait").isLessThan(10))
		            .assertNext(diff -> assertThat(diff).as("acquireOrWait(10) doesn't introduce significant wait").isLessThan(10))
		            .verifyComplete();
	}

	@Test
	public void delayWithAcquireWaitOverRateBursts() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		Mono<Tuple2<Long, Long>> totalTimeBeforeAfter =
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(250))
				    .elapsed()
				    .delayUntil(i -> testRateLimiter.acquireOrWait())
				    .elapsed()
				    .log()
				    .reduce(Tuples.of(0L, 0L),
						    (current, next) -> Tuples.of(
						    		current.getT1() + next.getT2().getT1(),
								    current.getT2() + next.getT1())
				    ).log();

		StepVerifier.create(totalTimeBeforeAfter)
		            .assertNext(beforeAfter -> {
		            	assertThat(10.0 / beforeAfter.getT1()).isGreaterThan(3.1 / 1000);
		            	assertThat(10.0 / beforeAfter.getT2()).isLessThanOrEqualTo(3.1 / 1000);
		            })
		            .verifyComplete();
	}

	@Test
	public void delayWithAcquireRejectUnderRate() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		Flux<List<Long>> source =
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(500))
				    .delayUntil(i -> testRateLimiter.acquireOrReject())
				    .elapsed().map(Tuple2::getT1)
				    .buffer(Duration.ofSeconds(1))
				    .log();

		StepVerifier.create(source)
		            .thenConsumeWhile(l -> l.stream().mapToLong(Long::longValue).sum() <= 1010L
				            && l.size() <= 3)
		            .verifyComplete();
	}


	@Test
	public void delayWithAcquireRejectOverRateFails() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		Flux<Integer> source =
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(250))
				    .delayUntil(i -> testRateLimiter.acquireOrReject())
				    .log(null, Level.INFO, SignalType.ON_NEXT);

		StepVerifier.create(source)
		            .expectNext(1, 2, 3)
		            .expectError(RateLimitedException.class)
		            .verify();
	}

	@Test
	public void filterWithTryAcquireUnderRate() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		Flux<List<Long>> source =
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(500))
				    .filterWhen(i -> testRateLimiter.tryAcquire())
				    .elapsed().map(Tuple2::getT1)
				    .buffer(Duration.ofSeconds(1))
				    .log();

		StepVerifier.create(source)
		            .thenConsumeWhile(l -> l.stream().mapToLong(Long::longValue).sum() <= 1010L
				            && l.size() <= 3)
		            .verifyComplete();
	}


	@Test
	public void filterWithTryAcquireOverRateDrops() {
		RateLimiter testRateLimiter = new LeakingBucketRateLimiter(Duration.ofSeconds(1),
				3, Schedulers.newSingle("testRateLimiter"));

		Flux<List<Integer>> source =
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(250))
				    .filterWhen(i -> testRateLimiter.tryAcquire())
				    .buffer(Duration.ofSeconds(1))
				    .log(null, Level.INFO, SignalType.ON_NEXT);

		StepVerifier.create(source)
		            .assertNext(l -> assertThat(l).containsExactly(1, 2, 3))
		            .assertNext(l -> assertThat(l).containsExactly(5, 6, 7))
		            .assertNext(l -> assertThat(l).containsExactly(9, 10))
		            .verifyComplete();
	}

}