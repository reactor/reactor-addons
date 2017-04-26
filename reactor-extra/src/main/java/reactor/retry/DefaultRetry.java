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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

public class DefaultRetry<T> extends AbstractRetry<T, Throwable> implements Retry<T> {

	static final Logger log = Loggers.getLogger(DefaultRetry.class);
	static final Consumer<? super RetryContext<?>> NOOP_ON_RETRY = r -> {};

	final Predicate<? super RetryContext<T>> retryPredicate;
	final Consumer<? super RetryContext<T>> onRetry;

	DefaultRetry(Predicate<? super RetryContext<T>> retryPredicate,
			int maxIterations,
			Duration timeout,
			Backoff backoff,
			Jitter jitter,
			Scheduler backoffScheduler,
			final Consumer<? super RetryContext<T>> onRetry,
			T applicationContext) {
		super(maxIterations, timeout, backoff, jitter, backoffScheduler,applicationContext);
		this.retryPredicate = retryPredicate;
		this.onRetry = onRetry;
	}

	public static <T> DefaultRetry<T> create(Predicate<? super RetryContext<T>> retryPredicate) {
		return new DefaultRetry<T>(retryPredicate,
				1,
				null,
				Backoff.zero(),
				Jitter.noJitter(),
				null,
				NOOP_ON_RETRY,
				(T) null);
	}

	@Override
	public Publisher<Long> apply(Flux<Throwable> errors) {
		return new RetryFunction().apply(errors);
	}

	@Override
	public Retry<T> withApplicationContext(T applicationContext) {
		return new DefaultRetry<>(retryPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRetry, applicationContext);
	}

	@Override
	public Retry<T> doOnRetry(Consumer<? super RetryContext<T>> onRetry) {
		return new DefaultRetry<>(retryPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRetry, applicationContext);
	}

	@Override
	public Retry<T> retryMax(int maxIterations) {
		if (maxIterations < 0)
			throw new IllegalArgumentException("maxIterations should be >= 0");
		return new DefaultRetry<>(retryPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRetry, applicationContext);
	}

	@Override
	public Retry<T> timeout(Duration timeout) {
		if (timeout.isNegative())
			throw new IllegalArgumentException("timeout should be >= 0");
		return new DefaultRetry<>(retryPredicate, Integer.MAX_VALUE, timeout,
				backoff, jitter, backoffScheduler, onRetry, applicationContext);
	}

	@Override
	public Retry<T> backoff(Backoff backoff) {
		return new DefaultRetry<>(retryPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRetry, applicationContext);
	}

	@Override
	public Retry<T> jitter(Jitter jitter) {
		return new DefaultRetry<>(retryPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRetry, applicationContext);
	}

	@Override
	public Retry<T> withBackoffScheduler(Scheduler scheduler) {
		return new DefaultRetry<>(retryPredicate, maxIterations, timeout,
				backoff, jitter, scheduler, onRetry, applicationContext);
	}

	class RetryFunction implements Function<Flux<Throwable>, Publisher<Long>> {

		DefaultContext<T> lastRetryContext;

		RetryFunction() {
			this.lastRetryContext = new DefaultContext<T>(applicationContext, 0, calculateTimeout(), null, null);;
		}

		@Override
		public Publisher<Long> apply(Flux<Throwable> errors) {
			return errors.zipWith(Flux.range(1, Integer.MAX_VALUE))
					.concatMap(tuple -> retry(tuple.getT1(), tuple.getT2()));
		}

		Publisher<Long> retry(Throwable e, long iteration) {
			DefaultContext<T> tmpContext = new DefaultContext<>(applicationContext, iteration, lastRetryContext.timeoutInstant, lastRetryContext.backoff, e);
			BackoffDelay nextBackoff = calculateBackoff(tmpContext);
			DefaultContext<T> retryContext = new DefaultContext<T>(applicationContext, iteration, lastRetryContext.timeoutInstant, nextBackoff, e);

			if (!retryPredicate.test(retryContext)) {
				log.debug("Stopping retries since predicate returned false, retry context: {}", retryContext);
				return Mono.error(e);
			}
			else if (nextBackoff == RETRY_EXHAUSTED) {
				log.debug("Retries exhausted, retry context: {}", retryContext);
				return Mono.error(new RetryExhaustedException(e));
			}
			else {
				log.debug("Scheduling retry attempt, retry context: {}", retryContext);
				this.lastRetryContext = retryContext;
				onRetry.accept(retryContext);
				return retryMono(nextBackoff.delay());
			}
		}
	}
}