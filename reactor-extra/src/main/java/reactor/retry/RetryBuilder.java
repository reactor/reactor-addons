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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * Retry utility to control retry or repeat of {@link Flux} or {@link Mono} using
 * <ul>
 *   <li>{@link Flux#retryWhen(Function)} </li>
 *   <li>{@link Flux#repeatWhen(Function)} </li>
 *   <li>{@link Mono#retryWhen(Function)} </li>
 *   <li>{@link Mono#repeatWhen(Function)} </li>
 *   <li>{@link Mono#repeatWhenEmpty(Function)} </li>
 *   <li>{@link Mono#repeatWhenEmpty(int, Function)} </li>
 * </ul>
 *
 * This class enables commonly used retry/repeat patterns:
 * <ul>
 *   <li><b>Retry conditions</b>
 *   <ul>
 *       <li>Any</li>
 *       <li>Retriable exceptions</li>
 *       <li>Non-retriable exceptions</li>
 *       <li>Custom predicate based on retry context</li>
 *   </ul></li>
 *   <li><b>Backoff strategy</b>
 *   <ul>
 *       <li>No backoff</li>
 *       <li>Fixed backoff</li>
 *       <li>Exponential backoff</li>
 *       <li>Random backoff with de-correlated jitter delay</li>
 *   </ul></li>
 *   <li><b>Retry limits</b>
 *   <ul>
 *       <li>Overall timeout</li>
 *       <li>Maximum number of attempts</li>
 *   </ul></li>
 *   <li><b>Rollbacks</b>
 *   <ul>
 *       <li>Retry callback for rollbacks</li>
 *       <li>Retry context with application state</li>
 *   </ul></li>
 * </ul>
 * <p>
 * Example usage:
 * <pre><code>
 *    builder = RetryBuilder.create(applicationContext)
 *                          .anyOf(SocketException.class)
 *                          .exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(60))
 *                          .doOnRetry(context -&gt; context.getApplicationContext().rollback());
 *    flux.as(retryBuilder::retry);
 * </code></pre>
 *
 */
public class RetryBuilder<T> {

	static final Logger log = Loggers.getLogger(RetryBuilder.class);

	static final Duration NO_RETRY = Duration.ofSeconds(-1);
	static final Consumer<? super RetryContext<?>> NOOP_ON_RETRY = r -> {};

	final RetryContext<T> retryContext;
	Predicate<? super RetryContext<T>> retryPredicate;
	Integer maxAttempts;
	Duration timeout;
	Function<? super RetryContext<T>, Duration> backoffCalculator;
	Function<? super RetryContext<T>, Duration> jitter;
	Scheduler backoffScheduler;
	Consumer<? super RetryContext<T>> onRetry;

	RetryBuilder(T applicationContext) {
		this.retryContext = new RetryContext<T>(applicationContext);
		any();
		noBackoff();
		this.onRetry = NOOP_ON_RETRY;
	}

	/**
	 * Creates a RetryBuilder without an application context.
	 * @return instance of {@link RetryBuilder}
	 */
	public static RetryBuilder<Object> create() {
		return new RetryBuilder<Object>(null);
	}



	/**
	 * Creates a RetryBuilder with an application context that may be used in
	 * rollback operations specified using {@link #doOnRetry(Consumer)}.
	 * @param applicationContext Application context
	 * @return instance of {@link RetryBuilder}
	 */
	public static <T> RetryBuilder<T> create(T applicationContext) {
		return new RetryBuilder<T>(applicationContext);
	}

	/**
	 * Configures retry for any exception. This is the default.
	 * The number of retries is limited by the number of attempts configured
	 * using {@link #maxAttempts(int)} and the overall timeout configured using
	 * {@link #timeout(Duration)}.
	 *
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> any() {
		retryPredicate = context -> true;
		return this;
	}

	/**
	 * Configures retries for errors resulting from any of the specified retriable
	 * exceptions. The number of retries is limited by the number of attempts configured
	 * using {@link #maxAttempts(int)} and the overall timeout configured using
	 * {@link #timeout(Duration)}.
	 *
	 * @param retriableExceptions Exception classes for which retry may be attempted.
	 * @return updated {@link RetryBuilder}
	 */
	@SafeVarargs
	public final RetryBuilder<T> anyOf(Class<? extends Throwable>... retriableExceptions) {
		retryPredicate = context -> {
			Throwable exception = context.getException();
			if (exception == null)
				return true;
			for (Class<? extends Throwable> clazz : retriableExceptions) {
				if (clazz.isInstance(exception))
					return true;
			}
			return false;
		};
		return this;
	}

	/**
	 * Configures retries for errors which are not any of the specified non-retriable
	 * exceptions. The number of retries is limited by the number of attempts configured
	 * using {@link #maxAttempts(int)} and the overall timeout configured using
	 * {@link #timeout(Duration)}.
	 *
	 * @param nonRetriableExceptions Exception classes for which retry is not attempted.
	 * @return updated {@link RetryBuilder}
	 */
	@SafeVarargs
	public final RetryBuilder<T> allBut(final Class<? extends Throwable>... nonRetriableExceptions) {
		retryPredicate = context -> {
			Throwable exception = context.getException();
			if (exception == null)
				return true;
			for (Class<? extends Throwable> clazz : nonRetriableExceptions) {
				if (clazz.isInstance(exception))
					return false;
			}
			return true;
		};
		return this;
	}

	/**
	 * Configures a custom predicate provided to determine if a repeat or
	 * retry may be attempted. The context is updated with the latest iteration count,
	 * exception if any as well as the next backoff interval before the predicate is
	 * invoked. Retries may be further limited by {@link #timeout(Duration)}
	 * and {@link #maxAttempts(int)}.
	 *
	 * @param predicate Predicate to determine if another retry may be performed
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> onlyIf(Predicate<RetryContext<T>> predicate) {
		retryPredicate = context -> predicate.test(context);
		maxAttempts = Integer.MAX_VALUE;
		return this;
	}

	/**
	 * Configures the number of attempts to one. No retry is performed.
	 * By default, no retries are performed if neither maxAttempts nor
	 * timeout is configured.
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> once() {
		this.maxAttempts = 1;
		return this;
	}

	/**
	 * Configures the maximum number of attempts including the first attempt prior to any
	 * retries. If overall timeout is also configured using {@link #timeout(Duration)},
	 * the number of attempts is limited by both maxAttempts and timeout. By default,
	 * no retries are attempted if neither maxAttempts nor timeout is configured.
	 * @param maxAttempts Maximum number of attempts
	 *
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> maxAttempts(int maxAttempts) {
		if (maxAttempts <= 0)
			throw new IllegalArgumentException("maxAttempts should be > 0");
		this.maxAttempts = maxAttempts;
		return this;
	}

	/**
	 * Configures the overall timeout starting from the instant {@link #buildRepeat()} or
	 * {@link #buildRetry()} is invoked. No retries are initiated beyond this timeout.
	 * If maxAttempts is also configured using {@link #maxAttempts(int)}, the number of
	 * attempts is limited by both maxAttempts and timeout. By default, no retries are
	 * attempted if neither maxAttempts nor timeout is configured.
	 *
	 * @param timeout Timeout for completion, no retries are attempted beyond this timeout.
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> timeout(Duration timeout) {
		if (timeout.isNegative())
			throw new IllegalArgumentException("timeout should be >= 0");
		this.timeout = timeout;
		return this;
	}

	/**
	 * Configures zero backoff interval. This is the default.
	 *
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> noBackoff() {
		this.backoffCalculator = context -> Duration.ZERO;
		this.jitter = this::noJitter;
		return this;
	}

	/**
	 * Configures a fixed backoff interval between retries. The scheduler used for backoff may
	 * be configured using {@link #backoffScheduler(Scheduler)}.
	 *
	 * @param backoffInterval delay between retries
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> fixedBackoff(Duration backoffInterval) {
		this.backoffCalculator = context -> backoffInterval;
		this.jitter = this::noJitter;
		return this;
	}

	/**
	 * Configures an exponential backoff interval between retries. The scheduler used for backoff may
	 * be configured using {@link #backoffScheduler(Scheduler)}. Retries are performed after a backoff
	 * interval of <code>firstBackoff * (2 ** n)</code> where n is the number of retries completed (attempts -1).
	 * If <code>maxBackoff</code> is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 * The total number of attempts can be limited by {@link #timeout(Duration)} and/or {@link #maxAttempts(int)}.
	 *
	 * @param firstBackoff delay for the first backoff, which is also used as the coefficient for subsequent backoffs
	 * @param maxBackoff the maximum backoff delay before a retry
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> exponentialBackoff(Duration firstBackoff, Duration maxBackoff) {
		if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero())
			throw new IllegalArgumentException("firstBackoff must be > 0");
		Duration maxBackoffInterval = maxBackoff != null ? maxBackoff : Duration.ofSeconds(Long.MAX_VALUE);
		if (maxBackoffInterval.compareTo(firstBackoff) <= 0)
			throw new IllegalArgumentException("maxBackoff must be >= firstBackoff");
		retryContext.setMinBackoff(firstBackoff);
		retryContext.setMaxBackoff(maxBackoffInterval);
		this.backoffCalculator = context -> {
			return firstBackoff.multipliedBy((long) Math.pow(2, (context.getAttempts() - 1)));
		};
		this.jitter = this::noJitter;
		return this;
	}

	/**
	 * Configures full jitter backoff strategy between retries. The scheduler used for backoff may
	 * be configured using {@link #backoffScheduler(Scheduler)}. Retries are performed after a backoff
	 * interval of <code>firstBackoff * (2 ** n)</code> where n is the number of retries completed (attempts -1).
	 * If <code>maxBackoff</code> is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 * The total number of attempts can be limited by {@link #timeout(Duration)} and/or {@link #maxAttempts(int)}.
	 *
	 * @param firstBackoff delay for the first backoff, which is also used as the coefficient for subsequent backoffs
	 * @param maxBackoff the maximum backoff delay before a retry
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> exponentialBackoffWithJitter(Duration firstBackoff, Duration maxBackoff) {
		exponentialBackoff(firstBackoff, maxBackoff);
		this.jitter = this::randomJitter;
		return this;
	}

	/**
	 * Configures a random de-correlated jitter backoff interval between retries. The scheduler used for backoff may
	 * be configured using {@link #backoffScheduler(Scheduler)}. Retries are performed after a backoff
	 * interval of <code>random_between(firstBackoff, prevBackoff * 3)</code>, with a minimum value of <code>firstBackoff</code>.
	 * If <code>maxBackoff</code> is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 * The total number of attempts can be limited by {@link #timeout(Duration)} and/or {@link #maxAttempts(int)}.
	 *
	 * @param firstBackoff delay for the first backoff, also used as minimum backoff
	 * @param maxBackoff the maximum backoff delay before a retry
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> randomBackoff(Duration firstBackoff, Duration maxBackoff) {
		if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero())
			throw new IllegalArgumentException("firstBackoff must be > 0");
		Duration maxBackoffInterval = maxBackoff != null ? maxBackoff : Duration.ofSeconds(Long.MAX_VALUE);
		if (maxBackoffInterval.compareTo(firstBackoff) <= 0)
			throw new IllegalArgumentException("maxBackoff must be >= firstBackoff");
		retryContext.setMinBackoff(firstBackoff);
		this.backoffCalculator = context -> {
			Duration prevBackoff = context.getBackoff() == null ? Duration.ZERO : context.getBackoff();
			Duration nextBackoff = prevBackoff.multipliedBy(3);
			return nextBackoff.compareTo(firstBackoff) < 0 ? firstBackoff : nextBackoff;
		};

		this.jitter = this::randomJitter;
		return this;
	}

	/**
	 * Configures a scheduler for backoff delays.
	 * @param scheduler a scheduler instance
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> backoffScheduler(Scheduler scheduler) {
		this.backoffScheduler = scheduler;
		return this;
	}

	/**
	 * Configures an <code>onRetry</code> callback that is invoked prior to every retry
	 * attempt, before any backoff delay. Any rollbacks that need to be performed before
	 * retry can be performed in the callback. Application context specified in
	 * {@link RetryBuilder#create(Object)} can be accessed using {@link RetryContext#getApplicationContext()}.
	 *
	 * @param onRetry callback for retry notification
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder<T> doOnRetry(Consumer<RetryContext<T>> onRetry) {
		this.onRetry = onRetry;
		return this;
	}

	/**
	 * Returns a function that may be used to control retries when used with:
	 * <ul>
	 *   <li>{@link Flux#retryWhen(Function)}</li>
	 *   <li>{@link Mono#retryWhen(Function)}</li>
	 * </ul>
	 * If a retry is not attempted because the exception is not retriable or the
	 * custom retry predicate returns false, the original exception from the last
	 * attempt is propagated as the error. If a retry is not attempted because
	 * retries have been exhausted due to timeout or maximum attempt limit,
	 * a {@link RetryExhaustedException} is generated with the original exception
	 * from the last attempt as the cause.
	 *
	 * @return {@link Function} providing a {@link Flux} signalling any error from the
	 *         source sequence and returning a {@link Publisher} companion.
	 */
	public Function<Flux<Throwable>, ? extends Publisher<?>> buildRetry() {
		return new RetryFunction<T>(this);
	}

	/**
	 * Returns a function that may be used to control repeats when used with:
	 * <ul>
	 * <li>{@link Flux#repeatWhen(Function)}</li>
	 * <li>{@link Mono#repeatWhen(Function)}</li>
	 * <li>{@link Mono#repeatWhenEmpty(Function)}</li>
	 * <li>{@link Mono#repeatWhenEmpty(int, Function)}</li>
	 * </ul>
	 *
	 * @return {@link Function} providing a {@link Flux} signalling any error from the
	 *         source sequence and returning a {@link Publisher} companion.
	 */
	public Function<Flux<Long>, ? extends Publisher<?>> buildRepeat() {
		return new RepeatFunction<T>(this);
	}

	/**
	 * Transforms the source into a {@link Flux} that retries errors using the retry
	 * configuration of this builder.
	 * <p>
	 * Example usage:
	 * <pre><code>
	 *    builder = RetryBuilder.create(applicationContext)
	 *                          .anyOf(SocketException.class)
	 *                          .exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(60))
	 *                          .doOnRetry(context -&gt; context.getApplicationContext().rollback());
	 *    flux.as(retryBuilder::retry);
	 * </code></pre>
	 *
	 * @param source the source publisher
	 * @return {@link Flux} with retries configured on this builder
	 */
	public <S> Flux<S> retry(Publisher<S> source) {
		return Flux.from(source).retryWhen(this.buildRetry());
	}


	/**
	 * Transforms the source into a repeating {@link Flux} using the repeat
	 * configuration of this builder.
	 * <p>
	 * Example usage:
	 *  <pre><code>
	 *    builder = RetryBuilder.create().maxAttempts(5);
	 *    flux.as(retryBuilder::repeat);
	 * </code></pre>
	 *
	 * @param source the source publisher
	 * @return {@link Flux} with repeats configured on this builder
	 */
	public <S> Flux<S> repeat(Publisher<S> source) {
		return Flux.from(source).repeatWhen(this.buildRepeat());
	}

	Duration noJitter(RetryContext<T> context) {
		return context.getBackoff();
	}

	Duration randomJitter(RetryContext<T> context) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		long backoffMs = context.getBackoff().toMillis();
		long minBackoffMs = context.getMinBackoff().toMillis();
		long jitterBackoffMs = backoffMs == minBackoffMs ? minBackoffMs : random.nextLong(minBackoffMs, backoffMs);
		return Duration.ofMillis(jitterBackoffMs);
	}

	abstract static class AbstractRetryFunction<T, R> implements Function<Flux<R>, Publisher<Long>> {

		final Predicate<? super RetryContext<T>> retryPredicate;
		final Instant timeoutInstant;
		final Function<? super RetryContext<T>, Duration> backoffCalculator;
		final Function<? super RetryContext<T>, Duration> jitter;
		final Scheduler backoffScheduler;
		final Consumer<? super RetryContext<T>> onRetry;
		final RetryContext<T> retryContext;
		final int maxAttempts;

		AbstractRetryFunction(RetryBuilder<T> builder) {
			this.retryContext = builder.retryContext;
			this.retryPredicate = builder.retryPredicate;
			this.backoffCalculator = builder.backoffCalculator;
			this.jitter = builder.jitter;
			this.backoffScheduler = builder.backoffScheduler;
			this.onRetry = builder.onRetry;
			this.maxAttempts = builder.maxAttempts != null ? builder.maxAttempts : builder.timeout == null ? 1 : Integer.MAX_VALUE;
			this.timeoutInstant = builder.timeout != null ? Instant.now().plus(builder.timeout) : Instant.MAX;
		}

		Duration calculateBackoff() {
			retryContext.setBackoff(backoffCalculator.apply(retryContext));
			Duration backoff = jitter.apply(retryContext);
			Duration minBackoff = retryContext.getMinBackoff();
			Duration maxBackoff = retryContext.getMaxBackoff();
			if (maxBackoff != null)
				backoff = backoff.compareTo(maxBackoff) < 0 ? backoff : maxBackoff;
			if (minBackoff != null)
				backoff = backoff.compareTo(minBackoff) > 0 ? backoff : minBackoff;
			retryContext.setBackoff(backoff);
			if (retryContext.attempts >= maxAttempts || Instant.now().plus(backoff).isAfter(timeoutInstant))
				return NO_RETRY;
			else
				return backoff;
		}

		Publisher<Long> retryMono(Duration delay) {
			if (delay == Duration.ZERO)
				return Mono.just(0L);
			else if (backoffScheduler == null)
				return Mono.delay(delay);
			else
				return Mono.delay(delay, backoffScheduler);
		}
	}

	static class RetryFunction<T> extends AbstractRetryFunction<T, Throwable> {
		RetryFunction(RetryBuilder<T> builder) {
			super(builder);
		}

		@Override
		public Publisher<Long> apply(Flux<Throwable> errors) {
			return errors.zipWith(Flux.range(1, Integer.MAX_VALUE))
					.concatMap(tuple -> retryWhen(tuple.getT2(), tuple.getT1()));
		}

		Publisher<Long> retryWhen(long attempts, Throwable e) {
			retryContext.setAttempts(attempts);
			retryContext.setException(e);
			Duration backoff = calculateBackoff();

			if (!retryPredicate.test(retryContext)) {
				log.debug("Stopping retries since predicate returned false, retry context: {}", retryContext);
				return Mono.error(e);
			}
			else if (backoff == NO_RETRY) {
				log.debug("Retries exhausted, retry context: {}", retryContext);
				return Mono.error(new RetryExhaustedException(e));
			}
			else {
				log.debug("Scheduling retry attempt, retry context: {}", retryContext);
				onRetry.accept(retryContext);
				return retryMono(backoff);
			}
		}
	}

	static class RepeatFunction<T> extends AbstractRetryFunction<T, Long> {
		RepeatFunction(RetryBuilder<T> builder) {
			super(builder);
		}

		@Override
		public Publisher<Long> apply(Flux<Long> companionValues) {

			EmitterProcessor<Long> emitter = EmitterProcessor.create();
			return companionValues.zipWith(Flux.range(1, Integer.MAX_VALUE))
							.concatMap(tuple -> repeatWhen(tuple.getT2(), emitter, tuple.getT1()))
							.zipWith(emitter, (t1, t2) -> t1);
		}

		Publisher<Long> repeatWhen(long attempts, EmitterProcessor<Long> emitter, Long companionValue) {
			retryContext.setAttempts(attempts);
			retryContext.setCompanionValue(companionValue);
			Duration backoff = calculateBackoff();

			if (!retryPredicate.test(retryContext)) {
				log.debug("Stopping repeats since predicate returned false, retry context: {}", retryContext);
				emitter.onComplete();
				return Mono.empty();
			}
			else if (backoff == NO_RETRY) {
				log.debug("Repeats exhausted, retry context: {}", retryContext);
				emitter.onComplete();
				return Mono.empty();
			}
			else {
				log.debug("Scheduling repeat attempt, retry context: {}", retryContext);
				onRetry.accept(retryContext);
				emitter.onNext(attempts);
				return retryMono(backoff);
			}
		}
	}
}
