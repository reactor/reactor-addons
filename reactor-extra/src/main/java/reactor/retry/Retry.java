/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.retry;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * Retry function that may be used with {@link Flux#retryWhen(reactor.util.retry.Retry)} and {@link
 * Mono#retryWhen(reactor.util.retry.Retry)}, after conversion via {@link reactor.util.retry.Retry#withThrowable(Function)}.
 * <p>
 * Each change in configuration returns a new instance (copy configuration), which
 * makes {@link Retry} suitable for creating configuration templates that can be fine
 * tuned for specific cases without impacting the original general use-case configuration.
 * <p>
 * Example usage:
 *
 * <pre><code>
 *   retry = Retry.anyOf(IOException.class)
 *                 .randomBackoff(Duration.ofMillis(100), Duration.ofSeconds(60))
 *                 .withApplicationContext(appContext)
 *                 .doOnRetry(context -> context.applicationContext().rollback());
 *   flux.retryWhen(retry);
 * </code></pre>
 *
 * @param <T> Application context type
 * @deprecated To be removed in 3.6.0 at the earliest. Use equivalent features of reactor-core like
 * {@link reactor.util.retry.RetrySpec} and {@link reactor.util.retry.RetryBackoffSpec} instead.
 */
@Deprecated
public interface Retry<T> extends Function<Flux<Throwable>, Publisher<Long>> {

	/**
	 * Returns a retry function that retries any exception, once.
	 * More constraints may be added using {@link #retryMax(long)} or {@link #timeout(Duration)}.
	 *
	 * @return retry function that retries on any exception
	 */
	static <T> Retry<T> any() {
		return DefaultRetry.<T>create(context -> true);
	}

	/**
	 * Returns a retry function that retries errors resulting from any of the
	 * specified exceptions, once.
	 * More constraints may be added using {@link #retryMax(long)}
	 * or {@link #timeout(Duration)}.
	 *
	 * @param retriableExceptions Exceptions that may be retried
	 * @return retry function that retries indefinitely, only for specified exceptions
	 */
	@SafeVarargs
	static <T> Retry<T> anyOf(Class<? extends Throwable>... retriableExceptions) {
		Predicate<? super RetryContext<T>> predicate = context -> {
			Throwable exception = context.exception();
			if (exception == null)
				return true;
			for (Class<? extends Throwable> clazz : retriableExceptions) {
				if (clazz.isInstance(exception))
					return true;
			}
			return false;
		};
		return DefaultRetry.<T>create(predicate);
	}

	/**
	 * Returns a retry function that retries errors resulting from all exceptions except
	 * the specified non-retriable exceptions, once.
	 * More constraints may be added using
	 * {@link #retryMax(long)} or {@link #timeout(Duration)}.
	 *
	 * @param nonRetriableExceptions exceptions that may not be retried
	 * @return retry function that retries all exceptions except the specified non-retriable exceptions.
	 */
	@SafeVarargs
	static <T> Retry<T> allBut(final Class<? extends Throwable>... nonRetriableExceptions) {
		Predicate<? super RetryContext<T>> predicate = context -> {
			Throwable exception = context.exception();
			if (exception == null)
				return true;
			for (Class<? extends Throwable> clazz : nonRetriableExceptions) {
				if (clazz.isInstance(exception))
					return false;
			}
			return true;
		};
		return DefaultRetry.<T>create(predicate);
	}

	/**
	 * Retry function that retries only if the predicate returns true, with no limit to
	 * the number of attempts.
	 * @param predicate Predicate that determines if next retry is performed
	 * @return Retry function with predicate
	 */
	static <T> Retry<T> onlyIf(Predicate<? super RetryContext<T>> predicate) {
		return DefaultRetry.create(predicate).retryMax(Long.MAX_VALUE);
	}

	/**
	 * Returns a retry function with an application context that may be
	 * used to perform any rollbacks before a retry. This application
	 * context is provided to any retry predicate {@link #onlyIf(Predicate)},
	 * custom backoff function {@link #backoff(Backoff)} and retry
	 * callback {@link #doOnRetry(Consumer)}. All other properties of
	 * this retry function are retained in the returned instance.
	 *
	 * @param applicationContext Application context
	 * @return retry function with associated application context
	 */
	Retry<T> withApplicationContext(T applicationContext);

	/**
	 * Returns a retry function that invokes the provided onRetry
	 * callback before every retry. The {@link RetryContext} provided
	 * to the callback contains the iteration and the any application
	 * context set using {@link #withApplicationContext(Object)}.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 *
	 * @param onRetry callback to invoke before retries
	 * @return retry function with callback
	 */
	Retry<T> doOnRetry(Consumer<? super RetryContext<T>> onRetry);

	/**
	 * Retry function that retries once.
	 * @return Retry function for one retry
	 */
	default Retry<T> retryOnce() {
		return retryMax(1);
	}

	/**
	 * Retry function that retries n times.
	 * @param maxRetries number of retries
	 * @return Retry function for n retries
	 */
	Retry<T> retryMax(long maxRetries);

	/**
	 * Returns a retry function with timeout. The timeout starts from
	 * the instant that this function is applied, and the function keeps retrying
	 * until the timeout expires (or until the configured maximum number of attempts, if
	 * it has been set). All other properties of this retry function are retained in
	 * the returned instance.
	 * @param timeout timeout after which no new retries are initiated
	 * @return retry function with global timeout
	 */
	Retry<T> timeout(Duration timeout);

	/**
	 * Returns a retry function with backoff delay.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 *
	 * @param backoff the backoff function to determine backoff delay
	 * @return retry function with backoff
	 */
	Retry<T> backoff(Backoff backoff);

	/**
	 * Returns a retry function that applies jitter to the backoff delay.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 *
	 * @param jitter Jitter function to randomize backoff delay
	 * @return retry function with jitter for backoff
	 */
	Retry<T> jitter(Jitter jitter);

	/**
	 * Returns a retry function that uses the scheduler provided for
	 * backoff delays. All other properties of this retry function
	 * are retained in the returned instance.
	 * @param scheduler the scheduler for backoff delays
	 * @return retry function with backoff scheduler
	 */
	Retry<T> withBackoffScheduler(Scheduler scheduler);

	/**
	 * Returns a retry function with no backoff delay. This is the default.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 *
	 * @return retry function with no backoff delay
	 */
	default Retry<T> noBackoff() {
		return backoff(Backoff.zero());
	}

	/**
	 * Returns a retry function with fixed backoff delay.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 *
	 * @param backoffInterval fixed backoff delay applied before every retry
	 * @return retry function with fixed backoff delay
	 */
	default Retry<T> fixedBackoff(Duration backoffInterval) {
		return backoff(Backoff.fixed(backoffInterval));
	}

	/**
	 * Returns a retry function with exponential backoff delay.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 * <p>
	 * Retries are performed after a backoff interval of <code>firstBackoff * (2 ** n)</code>
	 * where n is the next iteration number. If <code>maxBackoff</code> is not null, the maximum
	 * backoff applied will be limited to <code>maxBackoff</code>.
	 *
	 * @param firstBackoff the delay for the first backoff, which is also used as the coefficient for subsequent backoffs
	 * @param maxBackoff the maximum backoff delay before a retry
	 * @return retry function with exponential backoff delay
	 */
	default Retry<T> exponentialBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 2, false));
	}

	/**
	 * Returns a retry function with full jitter backoff strategy.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 * <p>
	 * Retries are performed after a random backoff interval between  <code>firstBackoff</code> and
	 * <code>firstBackoff * (2 ** n)</code> where n is the next iteration number. If <code>maxBackoff</code>
	 * is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 *
	 * @param firstBackoff the delay for the first backoff, which is also used as the coefficient for subsequent backoffs
	 * @param maxBackoff the maximum backoff delay before a retry
	 * @return retry function with full jitter backoff strategy
	 */
	default Retry<T> exponentialBackoffWithJitter(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 2, false)).jitter(Jitter.random());
	}

	/**
	 * Returns a retry function with random de-correlated jitter backoff strategy.
	 * All other properties of this retry function are retained in the
	 * returned instance.
	 * <p>
	 * Retries are performed after a backoff interval of <code>random_between(firstBackoff, prevBackoff * 3)</code>,
	 * with a minimum value of <code>firstBackoff</code>. If <code>maxBackoff</code>
	 * is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 *
	 * @param firstBackoff the delay for the first backoff, also used as minimum backoff
	 * @param maxBackoff the maximum backoff delay before a retry
	 * @return retry function with de-correlated jitter backoff strategy
	 */
	default Retry<T> randomBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 3, true)).jitter(Jitter.random());
	}

	/**
	 * Transforms the source into a retrying {@link Flux} based on the properties
	 * configured for this function.
	 * <p>
	 * Example usage:
	 * <pre><code>
	 *    retry = Retry.anyOf(IOException.class)
	 *                 .withApplicationContext(appContext)
	 *                 .doOnRetry(context -> context.applicationContext().rollback())
	 *                 .exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(60));
	 *    flux.as(retry);
	 * </code></pre>
	 *
	 * @param source the source publisher
	 * @return {@link Flux} with the retry properties of this retry function
	 */
	default <S> Flux<S> apply(Publisher<S> source) {
		return Flux.from(source).retryWhen(reactor.util.retry.Retry.withThrowable(this));
	}
}
