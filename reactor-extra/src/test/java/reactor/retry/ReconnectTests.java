/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.retry;

import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.scheduler.clock.SchedulerClock;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.junit.Assert.*;

public class ReconnectTests {

	private Queue<RetryContext<?>> retries = new ConcurrentLinkedQueue<>();

	@Test
	public void shouldReconnectShorterDelayIfSubscriberCameLater_WithVirtualTimeTest() {
		final TestPublisher publisher = TestPublisher.create();
		Queue<Instant> times = new LinkedList<>();
		Queue<Instant> resets = new LinkedList<>();
		Queue<Instant> reconnect = new LinkedList<>();

		final Queue<Mono<Integer>> monos = new LinkedList<>();
		monos.add(Mono.just(1).doOnSubscribe(__ -> times.add(SchedulerClock.of(Schedulers.single()).instant())));
		monos.add(Mono.just(2).doOnSubscribe(__ -> times.add(SchedulerClock.of(Schedulers.single()).instant())));


		// given
		final int minBackoff = 1;
		final int maxBackoff = 5;
		final int timeout = 10;

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();

		try {
			final Mono<Integer> reconnectableSource =
					Mono.defer(monos::poll)
					    .as(Reconnect::fromSource)
					    .reconnectMax(Long.MAX_VALUE)
					    .exponentialBackoff(Duration.ofSeconds(minBackoff), Duration.ofSeconds(maxBackoff))
					    .timeout(Duration.ofSeconds(timeout))
					    .doOnReconnect(__ -> reconnect.add(SchedulerClock.of(Schedulers.single()).instant()))
					    .build((e, s) -> {
					        resets.add(SchedulerClock.of(Schedulers.single())
						                             .instant());
					        publisher.mono()
						             .subscribe(null, null, s);
					    });

			// then
			StepVerifier.create(reconnectableSource)
			            .expectSubscription()
	                    .expectNext(1)
			            .expectComplete()
	                    .verify(Duration.ofSeconds(timeout));

			publisher.next(0);

			Assertions.assertThat(((DefaultReconnectMono)reconnectableSource).subscribers == DefaultReconnectMono.EMPTY_UNSUBSCRIBED);

			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(minBackoff));

			// then
			StepVerifier.create(reconnectableSource)
			            .expectSubscription()
			            .expectNext(2)
			            .expectComplete()
			            .verify(Duration.ofSeconds(timeout));

			// reset called the same time as subscription (since the subscriber
			// connected as second later the reconnection delay has already happened
			Assertions.assertThat(Duration.between(resets.peek(), times.peek())).isEqualTo(Duration.ZERO);
			Assertions.assertThat(Duration.between(resets.peek(), reconnect.peek())).isEqualTo(Duration.ZERO);

		} finally {
			VirtualTimeScheduler.reset();
		}
	}

	@Test
	public void shouldTimeoutReconnectWithVirtualTime() {
		final TestPublisher publisher = TestPublisher.create();
		// given
		final int minBackoff = 1;
		final int maxBackoff = 5;
		final int timeout = 10;

		// then
		StepVerifier.withVirtualTime(() ->
				Mono.<String>error(new RuntimeException("Something went wrong"))
					.as(Reconnect::fromSource)
					.reconnectMax(Long.MAX_VALUE)
					.exponentialBackoffWithJitter(Duration.ofSeconds(minBackoff), Duration.ofSeconds(maxBackoff))
					.timeout(Duration.ofSeconds(timeout))
					.build((e, s) -> publisher.mono().subscribe(null, null, s))
					.subscribeOn(Schedulers.elastic()))
				.expectSubscription()
//				.expectNoEvent(Duration.ofSeconds(timeout))
				.thenAwait(Duration.ofSeconds(timeout))
				.expectError(RetryExhaustedException.class)
				.verify(Duration.ofSeconds(timeout));
	}

	@Test
	public void reconnectNoBackoff() {
		final TestPublisher publisher = TestPublisher.create();
		Mono<Integer> mono = Mono.<Integer>error(new IOException("Something went wrong"))
		                         .as(Reconnect::fromSource)
		                         .noBackoff()
		                         .reconnectMax(2)
		                         .doOnReconnect(onRetry())
		                         .build((e, s) -> publisher.mono().subscribe(null, null, s));

		StepVerifier.create(mono)
		            .expectSubscription()
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class, IOException.class);
		RetryTestUtils.assertDelays(retries, 0L, 0L);
	}

	@Test
	public void monoRetryFixedBackoff() {
		final TestPublisher publisher = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> Mono.error(new IOException())
		                                       .as(Reconnect::fromSource)
		                                       .fixedBackoff(Duration.ofMillis(500))
		                                       .reconnectMax(1)
		                                       .doOnReconnect(onRetry())
		                                       .build((e, s) -> publisher.mono().subscribe(null, null, s)))
					.expectSubscription()
					.expectNoEvent(Duration.ofMillis(300))
					.thenAwait(Duration.ofMillis(300))
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class);
		RetryTestUtils.assertDelays(retries, 500L);
	}

	@Test
	public void monoRetryExponentialBackoff() {
		final TestPublisher publisher = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> Mono.error(new IOException())
		                                       .as(Reconnect::fromSource)
		                                       .exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
		                                       .reconnectMax(4)
		                                       .doOnReconnect(onRetry())
		                                       .build((e, s) -> publisher.mono().subscribe(null, null, s)))
					.expectSubscription()
					.thenAwait(Duration.ofMillis(100))
					.thenAwait(Duration.ofMillis(200))
					.thenAwait(Duration.ofMillis(400))
					.thenAwait(Duration.ofMillis(500))
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
		RetryTestUtils.assertDelays(retries, 100L, 200L, 400L, 500L);
	}

	@Test
	public void monoRetryRandomBackoff() {
		final TestPublisher publisher = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> Mono.error(new IOException())
		                                       .as(Reconnect::fromSource)
		                                       .randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
		                                       .reconnectMax(4)
		                                       .doOnReconnect(onRetry())
		                                       .build((e, s) -> publisher.mono().subscribe(null, null, s)))
					.expectSubscription()
					.thenAwait(Duration.ofMillis(100))
					.thenAwait(Duration.ofMillis(2000))
					.thenAwait(Duration.ofMillis(2000))
					.thenAwait(Duration.ofMillis(2000))
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
		RetryTestUtils.assertRandomDelays(retries, 100, 2000);
	}


	@Test
	public void retriablePredicate() {
		final TestPublisher publisher = TestPublisher.create();
		Mono<Integer> retriable = Mono.<Integer>error(new SocketException())
		                         .as(Reconnect::fromSource)
		                         .reconnectMax(1)
		                         .onlyIf(rc -> rc.exception() == null || rc.exception() instanceof SocketException)
		                         .doOnReconnect(onRetry())
		                         .build((e, s) -> publisher.mono().subscribe(null, null, s));

		StepVerifier.create(retriable)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		Mono<Integer> nonRetriable = Mono
				.<Integer>error(new RuntimeException())
				.as(Reconnect::fromSource)
				.reconnectMax(1)
				.doOnReconnect(onRetry())
				.onlyIf(rc -> rc.exception() == null || rc.exception() instanceof SocketException)
				.build((e, s) -> publisher.mono().subscribe(null, null, s));

		StepVerifier.create(nonRetriable)
					.verifyError(RuntimeException.class);

	}


	@Test
	public void doOnRetry() {
		Semaphore semaphore = new Semaphore(0);
		Retry<?> retry = Retry.any()
				.retryOnce()
				.fixedBackoff(Duration.ofMillis(500))
				.doOnRetry(context -> semaphore.release());

		StepVerifier.withVirtualTime(() -> Flux.range(0, 2).concatWith(Mono.error(new SocketException())).retryWhen(retry))
					.expectNext(0, 1)
					.then(() -> semaphore.acquireUninterruptibly())
					.expectNoEvent(Duration.ofMillis(400))
					.thenAwait(Duration.ofMillis(200))
					.expectNext(0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		StepVerifier.withVirtualTime(() -> Mono.error(new SocketException()).retryWhen(retry.noBackoff()))
					.then(() -> semaphore.acquireUninterruptibly())
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));
	}

	@Test
	public void retryApplicationContext() {
		class AppContext {
			boolean needsRollback;
			void rollback() {
				needsRollback = false;
			}
			void run() {
				assertFalse("Rollback not performed", needsRollback);
				needsRollback = true;
			}
		}
		AppContext appContext = new AppContext();
		Retry<?> retry = Retry.<AppContext>any().withApplicationContext(appContext)
				.retryMax(2)
				.doOnRetry(context -> {
					AppContext ac = context.applicationContext();
					assertNotNull("Application context not propagated", ac);
					ac.rollback();
				});

		StepVerifier.withVirtualTime(() -> Mono.error(new RuntimeException()).doOnNext(i -> appContext.run()).retryWhen(retry))
					.verifyErrorMatches(e -> isRetryExhausted(e, RuntimeException.class));

	}

	@Test
	public void fluxRetryCompose() {
		Retry<?> retry = Retry.any().noBackoff().retryMax(2).doOnRetry(this.onRetry());
		Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException())).as(retry::apply);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1, 0, 1)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class, IOException.class);
	}

	@Test
	public void monoRetryCompose() {
		Retry<?> retry = Retry.any().noBackoff().retryMax(2).doOnRetry(this.onRetry());
		Flux<?> flux = Mono.error(new IOException()).as(retry::apply);

		StepVerifier.create(flux)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class, IOException.class);
	}

	@Test
	public void functionReuseInParallel() throws Exception {
		int retryCount = 19;
		int range = 100;
		Integer[] values = new Integer[(retryCount + 1) * range];
		for (int i = 0; i <= retryCount; i++) {
			for (int j = 1; j <= range; j++)
				values[i * range + j - 1] = j;
		}
		RetryTestUtils.<Throwable>testReuseInParallel(2, 20,
				backoff -> Retry.<Integer>any().retryMax(19).backoff(backoff),
				retryFunc -> {
					StepVerifier.create(Flux.range(1, range).concatWith(Mono.error(new SocketException())).retryWhen(retryFunc))
								.expectNext(values)
								.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));
					});
	}

	Consumer<? super RetryContext<?>> onRetry() {
		return context -> retries.add(context);
	}

	@SafeVarargs
	private final void assertRetries(Class<? extends Throwable>... exceptions) {
		assertEquals(exceptions.length, retries.size());
		int index = 0;
		for (Iterator<RetryContext<?>> it = retries.iterator(); it.hasNext(); ) {
			RetryContext<?> retryContext = it.next();
			assertEquals(index + 1, retryContext.iteration());
			assertEquals(exceptions[index], retryContext.exception().getClass());
			index++;
		}
	}

	static boolean isRetryExhausted(Throwable e, Class<? extends Throwable> cause) {
		return e instanceof RetryExhaustedException && cause.isInstance(e.getCause());
	}

	@Test
	public void retryToString() {
		System.out.println(Retry.any().noBackoff().retryMax(2).toString());
	}
}
