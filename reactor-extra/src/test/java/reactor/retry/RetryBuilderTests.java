/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RetryBuilderTests {

	private ConcurrentLinkedQueue<RetryContext<?>> retries = new ConcurrentLinkedQueue<>();

	@Test
	public void fluxRetryNoBackoff() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(3);
		Flux<Integer> flux = createRetryFlux(2, new IOException(), builder);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1, 0, 1)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class, IOException.class);
		assertDelays(0L, 0L);
	}

	@Test
	public void fluxRepeatNoBackoff() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(3);
		Flux<Integer> flux = createRepeatFlux(2, builder);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1, 0, 1)
					.verifyComplete();
		assertRepeats(2L, 2L);
		assertDelays(0L, 0L);
	}

	@Test
	public void monoRetryNoBackoff() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(3);
		Mono<?> mono = createRetryMono( new IOException(), builder);

		StepVerifier.create(mono)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class, IOException.class);
		assertDelays(0L, 0L);
	}

	@Test
	public void monoRepeatNoBackoff() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(4);
		Flux<Integer> flux = createRepeatMono(builder);

		StepVerifier.create(flux)
					.expectNext(0, 0, 0, 0)
					.verifyComplete();
		assertRepeats(1L, 1L, 1L);
		assertDelays(0L, 0L, 0L);

		// Test with empty Mono
		retries.clear();
		StepVerifier.create(Mono.empty().repeatWhen(builder.buildRepeat()))
					.verifyComplete();
		assertRepeats(0L, 0L, 0L);
		assertDelays(0L, 0L, 0L);
	}

	@Test
	public void monoRepeatEmptyNoBackoff() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(4);
		Mono<Integer> mono = createRepeatEmptyMono(builder);

		StepVerifier.create(mono)
					.verifyComplete();
		assertRepeats(0L, 1L, 2L);
		assertDelays(0L, 0L, 0L);
	}

	@Test
	public void fluxRetryFixedBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.fixedBackoff(Duration.ofMillis(500))
				.maxAttempts(2);
		Flux<Integer> flux = createRetryFlux(2, new IOException(), builder);

		StepVerifier.withVirtualTime(() -> flux)
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(300))
					.thenAwait(Duration.ofMillis(300))
					.expectNext(0, 1)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class);
		assertDelays(500L);
	}

	@Test
	public void fluxRepeatFixedBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.fixedBackoff(Duration.ofMillis(500))
				.maxAttempts(2);
		Flux<Integer> flux = createRepeatFlux(2, builder);

		StepVerifier.withVirtualTime(() -> flux)
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(300))
					.thenAwait(Duration.ofMillis(300))
					.expectNext(0, 1)
					.verifyComplete();
		assertRepeats(2L);
		assertDelays(500L);
	}

	@Test
	public void monoRetryFixedBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.fixedBackoff(Duration.ofMillis(500))
				.maxAttempts(2);
		Mono<?> mono = createRetryMono( new IOException(), builder);

		StepVerifier.withVirtualTime(() -> mono)
					.expectSubscription()
					.expectNoEvent(Duration.ofMillis(300))
					.thenAwait(Duration.ofMillis(300))
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class);
		assertDelays(500L);
	}

	@Test
	public void monoRepeatFixedBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.fixedBackoff(Duration.ofMillis(500))
				.maxAttempts(2);
		Flux<Integer> flux = createRepeatMono(builder);

		StepVerifier.withVirtualTime(() ->flux)
					.expectNext(0)
					.expectNoEvent(Duration.ofMillis(300))
					.thenAwait(Duration.ofMillis(300))
					.expectNext(0)
					.verifyComplete();
		assertRepeats(1L);
		assertDelays(500L);
	}

	@Test
	public void monoRepeatEmptyFixedBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.fixedBackoff(Duration.ofMillis(500))
				.maxAttempts(2);
		Mono<Integer> mono = createRepeatEmptyMono(builder);

		StepVerifier.withVirtualTime(() -> mono)
					.expectSubscription()
					.expectNoEvent(Duration.ofMillis(300))
					.thenAwait(Duration.ofMillis(300))
					.verifyComplete();
		assertRepeats(0L);
		assertDelays(500L);
	}

	@Test
	public void fluxRetryExponentialBackoff() {
		RetryBuilder<?> builder = createBuilder()
			.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
			.timeout(Duration.ofMillis(1500));
		Flux<Integer> flux = createRetryFlux(2, new IOException(), builder);

		StepVerifier.create(flux)
		.expectNext(0, 1)
		.expectNoEvent(Duration.ofMillis(50))  // delay=100
		.expectNext(0, 1)
		.expectNoEvent(Duration.ofMillis(150)) // delay=200
		.expectNext(0, 1)
		.expectNoEvent(Duration.ofMillis(250)) // delay=400
		.expectNext(0, 1)
		.expectNoEvent(Duration.ofMillis(450)) // delay=500
		.expectNext(0, 1)
		.verifyErrorMatches(e -> isRetryExhausted(e, IOException.class));

		assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
		assertDelays(100L, 200L, 400L, 500L);
	}

	@Test
	public void fluxRepeatExponentialBackoff() {
		RetryBuilder<?> builder = createBuilder()
			.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
			.maxAttempts(5);
		Flux<Integer> flux = createRepeatFlux(2, builder);

		StepVerifier.withVirtualTime(() -> flux)
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(50))  // delay=100
					.thenAwait(Duration.ofMillis(100))
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(150)) // delay=200
					.thenAwait(Duration.ofMillis(100))
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(250)) // delay=400
					.thenAwait(Duration.ofMillis(100))
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(450)) // delay=500
					.thenAwait(Duration.ofMillis(100))
					.expectNext(0, 1)
					.verifyComplete();

		assertRepeats(2L, 2L, 2L, 2L);
		assertDelays(100L, 200L, 400L, 500L);
	}

	@Test
	public void monoRetryExponentialBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
				.maxAttempts(5);
		Mono<?> mono = createRetryMono( new IOException(), builder);

		StepVerifier.withVirtualTime(() -> mono)
					.expectSubscription()
					.thenAwait(Duration.ofMillis(100))
					.thenAwait(Duration.ofMillis(200))
					.thenAwait(Duration.ofMillis(400))
					.thenAwait(Duration.ofMillis(500))
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
		assertDelays(100L, 200L, 400L, 500L);
	}

	@Test
	public void monoRepeatExponentialBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
				.maxAttempts(5);
		Flux<Integer> flux = createRepeatMono(builder);

		StepVerifier.withVirtualTime(() -> flux)
					.expectNext(0)
					.thenAwait(Duration.ofMillis(100))
					.expectNext(0)
					.thenAwait(Duration.ofMillis(200))
					.expectNext(0)
					.thenAwait(Duration.ofMillis(400))
					.expectNext(0)
					.thenAwait(Duration.ofMillis(500))
					.expectNext(0)
					.verifyComplete();
		assertRepeats(1L, 1L, 1L, 1L);
		assertDelays(100L, 200L, 400L, 500L);
	}

	@Test
	public void monoRepeatEmptyExponentialBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
				.maxAttempts(5);
		Mono<Integer> mono = createRepeatEmptyMono(builder);

		StepVerifier.withVirtualTime(() -> mono)
					.expectSubscription()
					.thenAwait(Duration.ofMillis(100))
					.thenAwait(Duration.ofMillis(200))
					.thenAwait(Duration.ofMillis(400))
					.thenAwait(Duration.ofMillis(500))
					.verifyComplete();

		assertRepeats(0L, 1L, 2L, 3L);
		assertDelays(100L, 200L, 400L, 500L);
	}

	@Test
	public void fluxRetryRandomBackoff() {
		RetryBuilder<?> builder = createBuilder()
			.randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
			.maxAttempts(5);
		Flux<Integer> flux = createRetryFlux(2, new IOException(), builder);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, IOException.class));

		assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
		assertRandomDelays(100, 2000);
	}


	@Test
	public void fluxRepeatRandomBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.randomBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
				.maxAttempts(5);
		Flux<Integer> flux = createRepeatFlux(2, builder);

		StepVerifier.withVirtualTime(() -> flux)
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(90))
					.thenAwait(Duration.ofMillis(50))
					.expectNext(0, 1)
					.thenAwait(Duration.ofMillis(500))
					.expectNext(0, 1)
					.thenAwait(Duration.ofMillis(500))
					.expectNext(0, 1)
					.thenAwait(Duration.ofMillis(500))
					.expectNext(0, 1)
					.verifyComplete();

		assertRepeats(2L, 2L, 2L, 2L);
		assertRandomDelays(100, 500);
	}

	@Test
	public void monoRetryRandomBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
				.maxAttempts(5);
		Mono<?> mono = createRetryMono( new IOException(), builder);

		StepVerifier.withVirtualTime(() -> mono)
					.expectSubscription()
					.thenAwait(Duration.ofMillis(100))
					.thenAwait(Duration.ofMillis(2000))
					.thenAwait(Duration.ofMillis(2000))
					.thenAwait(Duration.ofMillis(2000))
					.verifyError(RetryExhaustedException.class);

		assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
		assertRandomDelays(100, 2000);
	}

	@Test
	public void monoRepeatRandomBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
				.maxAttempts(5);
		Flux<Integer> flux = createRepeatMono(builder);

		StepVerifier.withVirtualTime(() -> flux)
					.expectNext(0)
					.thenAwait(Duration.ofMillis(100))
					.expectNext(0)
					.thenAwait(Duration.ofMillis(2000))
					.expectNext(0)
					.thenAwait(Duration.ofMillis(2000))
					.expectNext(0)
					.thenAwait(Duration.ofMillis(2000))
					.expectNext(0)
					.verifyComplete();
		assertRepeats(1L, 1L, 1L, 1L);
		assertRandomDelays(100, 2000);
	}

	@Test
	public void monoRepeatEmptyRandomBackoff() {
		RetryBuilder<?> builder = createBuilder()
				.randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
				.maxAttempts(5);
		Mono<Integer> mono = createRepeatEmptyMono(builder);

		StepVerifier.withVirtualTime(() -> mono)
					.expectSubscription()
					.thenAwait(Duration.ofMillis(100))
					.thenAwait(Duration.ofMillis(2000))
					.thenAwait(Duration.ofMillis(2000))
					.thenAwait(Duration.ofMillis(2000))
					.verifyComplete();

		assertRepeats(0L, 1L, 2L, 3L);
		assertRandomDelays(100, 2000);
	}

	@Test
	public void fluxRetriableExceptions() {
		RetryBuilder<?> builder = createBuilder().anyOf(IOException.class).maxAttempts(2);
		Flux<Integer> flux = createRetryFlux(2, new SocketException(), builder);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		Flux<Integer> nonRetriable = createRetryFlux(2, new RuntimeException(), builder);
		StepVerifier.create(nonRetriable)
					.expectNext(0, 1)
					.verifyError(RuntimeException.class);

	}

	@Test
	public void fluxNonRetriableExceptions() {

		RetryBuilder<?> builder = createBuilder().allBut(RuntimeException.class).maxAttempts(2);

		Flux<Integer> flux = createRetryFlux(2, new IllegalStateException(), builder);
		StepVerifier.create(flux)
					.expectNext(0, 1)
					.verifyError(IllegalStateException.class);


		Flux<Integer> retriable = createRetryFlux(2, new SocketException(), builder);
		StepVerifier.create(retriable)
					.expectNext(0, 1, 0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));
	}

	@Test
	public void fluxRetryAnyException() {
		RetryBuilder<?> builder = createBuilder().any().maxAttempts(2);
		Flux<Integer> flux = createRetryFlux(2, new SocketException(), builder);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		Flux<Integer> nonRetriable = createRetryFlux(2, new RuntimeException(), builder);
		StepVerifier.create(nonRetriable)
					.expectNext(0, 1, 0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, RuntimeException.class));

	}

	@Test
	public void fluxRetryOnPredicate() {

		RetryBuilder<?> builder = createBuilder().onlyIf(context -> context.getAttempts() < 3);
		Flux<Integer> flux = createRetryFlux(2, new SocketException(), builder);

		StepVerifier.create(flux)
			.expectNext(0, 1, 0, 1, 0, 1)
			.verifyError(SocketException.class);
	}

	@Test
	public void fluxRepeatOnPredicate() {

		RetryBuilder<?> builder = createBuilder().onlyIf(context -> context.getAttempts() < 3);
		Flux<Integer> flux = createRepeatFlux(2, builder);

		StepVerifier.create(flux)
			.expectNext(0, 1, 0, 1, 0, 1)
			.verifyComplete();
	}

	@Test
	public void attemptOnce() {
		RetryBuilder<?> builder = createBuilder().anyOf(IOException.class).once();

		StepVerifier.create(createRetryFlux(2, new SocketException(), builder))
					.expectNext(0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		StepVerifier.create(createRepeatFlux(2, builder))
		.expectNext(0, 1)
		.verifyComplete();
	}

	@Test
	public void backoffScheduler() {
		Scheduler backoffScheduler = Schedulers.newSingle("test");
		RetryBuilder<?> builder = createBuilder()
				.maxAttempts(2)
				.fixedBackoff(Duration.ofMillis(100))
				.backoffScheduler(backoffScheduler);

		Semaphore semaphore = new Semaphore(0);
		backoffScheduler.schedule(() -> semaphore.acquireUninterruptibly());
		StepVerifier.withVirtualTime(() -> createRetryFlux(2, new SocketException(), builder))
					.expectNext(0, 1)
					.expectNoEvent(Duration.ofMillis(200))
					.then(() -> semaphore.release())
					.expectNext(0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		backoffScheduler.schedule(() -> semaphore.acquireUninterruptibly());
		StepVerifier.withVirtualTime(() -> createRepeatMono(builder))
					.expectNext(0)
					.expectNoEvent(Duration.ofMillis(200))
					.then(() -> semaphore.release())
					.expectNext(0)
					.verifyComplete();
	}

	@Test
	public void doOnRetry() {
		Semaphore semaphore = new Semaphore(0);
		RetryBuilder<?> builder = createBuilder()
				.maxAttempts(2)
				.fixedBackoff(Duration.ofMillis(500))
				.doOnRetry(context -> semaphore.release());

		StepVerifier.withVirtualTime(() -> Flux.range(0, 2).concatWith(Mono.error(new SocketException())).retryWhen(builder.buildRetry()))
					.expectNext(0, 1)
					.then(() -> semaphore.acquireUninterruptibly())
					.expectNoEvent(Duration.ofMillis(400))
					.thenAwait(Duration.ofMillis(200))
					.expectNext(0, 1)
					.verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

		StepVerifier.withVirtualTime(() -> Mono.just(0).repeatWhen(builder.buildRepeat()))
					.expectNext(0)
					.then(() -> semaphore.acquireUninterruptibly())
					.expectNoEvent(Duration.ofMillis(400))
					.thenAwait(Duration.ofMillis(200))
					.expectNext(0)
					.verifyComplete();
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
		RetryBuilder<?> builder = RetryBuilder.create(appContext)
				.maxAttempts(3)
				.doOnRetry(context -> {
					AppContext ac = (AppContext) context.getApplicationContext();
					assertNotNull("Application context not propagated", ac);
					ac.rollback();
				});

		StepVerifier.withVirtualTime(() -> Mono.just(1).doOnNext(i -> appContext.run()).repeatWhen(builder.buildRepeat()))
					.expectNext(1, 1, 1)
					.verifyComplete();

	}

	@Test
	public void fluxRetryCompose() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(3);
		Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException())).as(builder::retry);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1, 0, 1)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class, IOException.class);
	}

	@Test
	public void fluxRepeatCompose() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(3);
		Flux<Integer> flux = Flux.range(0, 2).as(builder::repeat);

		StepVerifier.create(flux)
					.expectNext(0, 1, 0, 1, 0, 1)
					.verifyComplete();
		assertRepeats(2L, 2L);
	}

	@Test
	public void monoRetryCompose() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(3);
		Flux<?> flux = Mono.error(new IOException()).as(builder::retry);

		StepVerifier.create(flux)
					.verifyError(RetryExhaustedException.class);
		assertRetries(IOException.class, IOException.class);
	}

	@Test
	public void monoRepeatCompose() {
		RetryBuilder<?> builder = createBuilder().noBackoff().maxAttempts(4);
		Flux<Integer> flux = Mono.just(5).as(builder::repeat);

		StepVerifier.create(flux)
					.expectNext(5, 5, 5, 5)
					.verifyComplete();
		assertRepeats(1L, 1L, 1L);

		// Test with empty Mono
		retries.clear();
		StepVerifier.create(Mono.empty().as(builder::repeat))
					.verifyComplete();
		assertRepeats(0L, 0L, 0L);
	}

	private RetryBuilder<?> createBuilder() {
		return RetryBuilder.create()
				.doOnRetry(c -> retries.add(c.clone()));

	}

	@SafeVarargs
	private final void assertRetries(Class<? extends Throwable>... exceptions) {
		assertEquals(exceptions.length, retries.size());
		int index = 0;
		for (Iterator<RetryContext<?>> it = retries.iterator(); it.hasNext(); ) {
			RetryContext<?> retryContext = it.next();
			assertEquals(index + 1, retryContext.getAttempts());
			assertEquals(exceptions[index], retryContext.getException().getClass());
			index++;
		}
	}

	private final void assertRepeats(Long... values) {
		assertEquals(values.length, retries.size());
		int index = 0;
		for (Iterator<RetryContext<?>> it = retries.iterator(); it.hasNext(); ) {
			RetryContext<?> retryContext = it.next();
			assertEquals(index + 1, retryContext.getAttempts());
			assertNull(retryContext.getException());
			assertEquals(values[index], retryContext.getCompanionValue());
			index++;
		}
	}

	private final void assertDelays(Long... delayMs) {
		assertEquals(delayMs.length, retries.size());
		int index = 0;
		for (Iterator<RetryContext<?>> it = retries.iterator(); it.hasNext(); ) {
			RetryContext<?> retryContext = it.next();
			assertEquals(delayMs[index].longValue(), retryContext.getBackoff().toMillis());
			index++;
		}
	}

	private void assertRandomDelays(int firstMs, int maxMs) {
		long prevMs = 0;
		int randomValues = 0;
		for (RetryContext<?> context : retries) {
			long backoffMs = context.getBackoff().toMillis();
			if (prevMs == 0)
				assertEquals(firstMs, backoffMs);
			else
				assertTrue("Unexpected delay " + backoffMs, backoffMs >= firstMs && backoffMs <= maxMs);
			if (backoffMs != firstMs && backoffMs != prevMs)
				randomValues++;
			prevMs = backoffMs;
		}
		assertTrue("Delays not random", randomValues >= 2); // Allow for at most one edge case.
	}

	private Flux<Integer> createRetryFlux(int exceptionIndex, Throwable exception, RetryBuilder<?> builder) {
		return Flux.concat(Flux.range(0, exceptionIndex), Flux.error(exception))
				.retryWhen(builder.buildRetry());
	}

	private Mono<?> createRetryMono(Throwable exception, RetryBuilder<?> builder) {
		return Mono.error(exception)
				.retryWhen(builder.buildRetry());
	}

	private Flux<Integer> createRepeatFlux(int count, RetryBuilder<?> builder) {
		return Flux.range(0, count)
				.repeatWhen(builder.buildRepeat());
	}

	private Flux<Integer> createRepeatMono(RetryBuilder<?> builder) {
		return Mono.just(0)
				.repeatWhen(builder.buildRepeat());
	}

	private Mono<Integer> createRepeatEmptyMono(RetryBuilder<?> builder) {
		return Mono.<Integer>empty()
				.repeatWhenEmpty(builder.buildRepeat());
	}

	private boolean isRetryExhausted(Throwable e, Class<? extends Throwable> cause) {
		return e instanceof RetryExhaustedException && cause.isInstance(e.getCause());
	}
}
