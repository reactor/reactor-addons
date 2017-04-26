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
 *
 */
public class RetryBuilder {

	static final Logger log = Loggers.getLogger(RetryBuilder.class);

	static final Duration NO_RETRY = Duration.ofSeconds(-1);

	final Predicate<RetryContext> retryPredicate;
	Integer maxAttempts;
	Duration timeout;
	Instant timeoutInstant;
	Function<RetryContext, Duration> backoffCalculator;
	Scheduler backoffScheduler;
	Consumer<RetryContext> onRetry;
	RetryContext retryContext;

	RetryBuilder(Predicate<RetryContext> retryPredicate) {
		this.retryPredicate = retryPredicate;
		this.maxAttempts = null;
		this.onRetry = r -> {};
		this.retryContext = new RetryContext(null);
		noBackoff();
	}

	/**
	 * Creates a RetryBuilder that retries based on the number of attempts configured
	 * using {@link #maxAttempts(int)} and the overall timeout configured using
	 * {@link #timeout(Duration)}. Any exception may be retried.
	 * @return
	 */
	public static RetryBuilder any() {
		return new RetryBuilder(context -> true);
	}

	/**
	 * Creates a RetryBuilder that retries errors resulting from any of the specified retriable
	 * exceptions. The number of retries is limited by the number of attempts configured
	 * using {@link #maxAttempts(int)} and the overall timeout configured using
	 * {@link #timeout(Duration)}.
	 *
	 * @param retriableExceptions Exception classes for which retry may be attempted.
	 * @return instance of RetryBuilder that retries only the specified exceptions
	 */
	@SafeVarargs
	public static RetryBuilder anyOf(Class<? extends Throwable>... retriableExceptions) {
		return new RetryBuilder(context -> {
			Throwable exception = context.getException();
			if (exception == null)
				return true;
			for (Class<? extends Throwable> clazz : retriableExceptions) {
				if (clazz.isInstance(exception))
					return true;
			}
			return false;
		});
	}

	/**
	 * Creates a RetryBuilder that retries errors which are not any of the specified non-retriable
	 * exceptions. The number of retries is limited by the number of attempts configured
	 * using {@link #maxAttempts(int)} and the overall timeout configured using
	 * {@link #timeout(Duration)}.
	 *
	 * @param nonRetriableExceptions Exception classes for which retry is not attempted.
	 * @return instance of RetryBuilder that retries all exceptions except the specified non-retriable exceptions
	 */
	@SafeVarargs
	public static RetryBuilder allBut(final Class<? extends Throwable>... nonRetriableExceptions) {
		return new RetryBuilder(context -> {
			Throwable exception = context.getException();
			if (exception == null)
				return true;
			for (Class<? extends Throwable> clazz : nonRetriableExceptions) {
				if (clazz.isInstance(exception))
					return false;
			}
			return true;
		});
	}

	/**
	 * Creates a RetryBuilder that uses the predicate provided to determine if a
	 * repeat or retry may be attempted. The context is updated with the latest
	 * iteration count, exception if any as well as the next backoff interval before
	 * the predicate is invoked. Retries may be further limited by {@link #timeout(Duration)}
	 * and {@link #maxAttempts(int)}.
	 *
	 * @param predicate Predicate to determine if another retry may be performed
	 * @return instance of RetryBuilder that retries only if predicate returns true
	 */
	public static RetryBuilder onlyIf(Predicate<RetryContext> predicate) {
		return new RetryBuilder(context -> predicate.test(context)).maxAttempts(Integer.MAX_VALUE);
	}

	/**
	 * Configures the number of attempts to one. No retry is performed.
	 * By default, no retries are performed if neither maxAttempts nor
	 * timeout is configured.
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder once() {
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
	public RetryBuilder maxAttempts(int maxAttempts) {
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
	public RetryBuilder timeout(Duration timeout) {
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
	public RetryBuilder noBackoff() {
		this.backoffCalculator = context -> Duration.ZERO;
		return this;
	}

	/**
	 * Configures a fixed backoff interval between retries. The scheduler used for backoff may
	 * be configured using {@link #backoffScheduler(Scheduler)}.
	 *
	 * @param backoffInterval delay between retries
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder fixedBackoff(Duration backoffInterval) {
		this.backoffCalculator = context -> backoffInterval;
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
	public RetryBuilder exponentialBackoff(Duration firstBackoff, Duration maxBackoff) {
		if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero())
			throw new IllegalArgumentException("firstBackoff must be > 0");
		Duration maxBackoffInterval = maxBackoff != null ? maxBackoff : Duration.ofSeconds(Long.MAX_VALUE);
		if (maxBackoffInterval.compareTo(firstBackoff) <= 0)
			throw new IllegalArgumentException("maxBackoff must be >= firstBackoff");
		this.backoffCalculator = context -> {
			Duration expBackoff = firstBackoff.multipliedBy((long) Math.pow(2, (context.getAttempts() - 1)));
			Duration backoff = expBackoff.compareTo(maxBackoffInterval) < 0 ? expBackoff : maxBackoffInterval;
			return backoff;
		};
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
	public RetryBuilder randomBackoff(Duration firstBackoff, Duration maxBackoff) {
		if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero())
			throw new IllegalArgumentException("firstBackoff must be > 0");
		long firstBackoffMs = firstBackoff.toMillis();
		long maxBackoffMs = maxBackoff != null ? maxBackoff.toMillis() : Long.MAX_VALUE;
		if (maxBackoffMs < firstBackoffMs)
			throw new IllegalArgumentException("maxBackoff must be >= firstBackoff");
		ThreadLocalRandom random = ThreadLocalRandom.current();
		this.backoffCalculator = context -> {
			long prevBackoffMs = context.getBackoff() == null ? 0 : context.getBackoff().toMillis();
			long maxMs = Math.max(firstBackoffMs, prevBackoffMs * 3);
			long jitterBackoffMs = firstBackoffMs == maxMs ? firstBackoffMs : random.nextLong(firstBackoffMs, maxMs);
			return Duration.ofMillis(Math.min(maxBackoffMs, jitterBackoffMs));
		};
		return this;
	}

	/**
	 * Configures a scheduler for backoff delays.
	 * @param scheduler a scheduler instance
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder backoffScheduler(Scheduler scheduler) {
		this.backoffScheduler = scheduler;
		return this;
	}

	/**
	 * Configures a retry context which may include application context. Applications which need access to
	 * application state for rollbacks may set a retry context for easy access to the state in
	 * {@link #doOnRetry(Consumer)} callbacks.
	 *
	 * @param retryContext RetryContext instance which is included in {@link #doOnRetry(Consumer)} callbacks
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder retryContext(RetryContext retryContext) {
		this.retryContext = retryContext;
		return this;
	}

	/**
	 * Configures an <code>onRetry</code> callback that is invoked prior to every retry
	 * attempt, before any backoff delay. Any rollbacks that need to be performed before
	 * retry can be performed in the callback. Application context required for rollback
	 * may be configured using {@link #retryContext(RetryContext)}.
	 *
	 * @param onRetry callback for retry notification
	 * @return updated {@link RetryBuilder}
	 */
	public RetryBuilder doOnRetry(Consumer<RetryContext> onRetry) {
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
		initialize();
		return errors -> errors.zipWith(Flux.range(1, Integer.MAX_VALUE)).flatMap(tuple -> retryWhen(tuple.getT2(), tuple.getT1()));
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
		EmitterProcessor<Long> emitter = EmitterProcessor.create();
		initialize();
		return companionValues -> companionValues.zipWith(Flux.range(1, Integer.MAX_VALUE))
				.flatMap(tuple -> repeatWhen(tuple.getT2(), emitter, tuple.getT1()))
				.zipWith(emitter, (t1, t2) -> t1);
	}

	private void initialize() {
		if (maxAttempts == null)
			maxAttempts = timeout == null ? 1 : Integer.MAX_VALUE;
		timeoutInstant = timeout != null ? Instant.now().plus(timeout) : Instant.MAX;
	}

	private Duration calculateBackoff() {
		Duration backoff = backoffCalculator.apply(retryContext);
		retryContext.setBackoff(backoff);
		if (retryContext.getAttempts() >= maxAttempts)
			return NO_RETRY;
		else if (Instant.now().plus(backoff).isAfter(timeoutInstant))
			return NO_RETRY;
		else
			return backoff;
	}

	private Publisher<?> retryMono(Duration delay) {
		if (delay == Duration.ZERO)
			return Mono.just(0);
		else if (backoffScheduler == null)
			return Mono.delay(delay);
		else
			return Mono.delay(delay, backoffScheduler);
	}

	private Publisher<?> retryWhen(long attempts, Throwable e) {
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

	private Publisher<?> repeatWhen(long attempts, EmitterProcessor<Long> emitter, Long companionValue) {
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
