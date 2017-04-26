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
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

public class DefaultRepeat<T> extends AbstractRetry<T, Long> implements Repeat<T> {

	static final Logger log = Loggers.getLogger(DefaultRepeat.class);
	static final Consumer<? super RepeatContext<?>> NOOP_ON_REPEAT = r -> {};

	final Predicate<? super RepeatContext<T>> repeatPredicate;
	final Consumer<? super RepeatContext<T>> onRepeat;

	DefaultRepeat(Predicate<? super RepeatContext<T>> repeatPredicate,
			int maxRepeats,
			Duration timeout,
			Backoff backoff,
			Jitter jitter,
			Scheduler backoffScheduler,
			final Consumer<? super RepeatContext<T>> onRepeat,
			T applicationContext) {
		super(maxRepeats, timeout, backoff, jitter, backoffScheduler, applicationContext);
		this.repeatPredicate = repeatPredicate;
		this.onRepeat = onRepeat;
	}

	public static <T> DefaultRepeat<T> create(Predicate<? super RepeatContext<T>> repeatPredicate, int n) {
		return new DefaultRepeat<T>(repeatPredicate,
				n,
				null,
				Backoff.zero(),
				Jitter.noJitter(),
				null,
				NOOP_ON_REPEAT,
				(T) null);
	}

	@Override
	public Publisher<Long> apply(Flux<Long> companionValues) {
		return new RepeatFunction().apply(companionValues);
	}

	@Override
	public Repeat<T> withApplicationContext(T applicationContext) {
		return new DefaultRepeat<>(repeatPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRepeat, applicationContext);
	}

	@Override
	public Repeat<T> doOnRepeat(Consumer<? super RepeatContext<T>> onRepeat) {
		return new DefaultRepeat<>(repeatPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRepeat, applicationContext);
	}

	@Override
	public Repeat<T> timeout(Duration timeout) {
		if (timeout.isNegative())
			throw new IllegalArgumentException("timeout should be >= 0");
		return new DefaultRepeat<>(repeatPredicate, Integer.MAX_VALUE, timeout,
				backoff, jitter, backoffScheduler, onRepeat, applicationContext);
	}

	@Override
	public Repeat<T> backoff(Backoff backoff) {
		return new DefaultRepeat<>(repeatPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRepeat, applicationContext);
	}

	@Override
	public Repeat<T> jitter(Jitter jitter) {
		return new DefaultRepeat<>(repeatPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRepeat, applicationContext);
	}

	@Override
	public Repeat<T> withBackoffScheduler(Scheduler scheduler) {
		return new DefaultRepeat<>(repeatPredicate, maxIterations, timeout,
				backoff, jitter, scheduler, onRepeat, applicationContext);
	}

	class RepeatFunction implements Function<Flux<Long>, Publisher<Long>> {

		DefaultContext<T> lastRepeatContext;

		public Publisher<Long> apply(Flux<Long> companionValues) {
			this.lastRepeatContext = new DefaultContext<T>(applicationContext, 0, calculateTimeout(), null, -1L);
			return companionValues
					.zipWith(Flux.range(1, Integer.MAX_VALUE), this::repeatBackoff)
					.takeWhile(backoff -> backoff != RETRY_EXHAUSTED)
					.concatMap(backoff -> retryMono(backoff.delay));
		}

		BackoffDelay repeatBackoff(Long companionValue, Integer iteration) {
			DefaultContext<T> tmpCpntext = new DefaultContext<>(applicationContext, iteration, lastRepeatContext.timeoutInstant, lastRepeatContext.backoff, companionValue);
			BackoffDelay nextBackoff = calculateBackoff(tmpCpntext);
			DefaultContext<T> repeatContext = new DefaultContext<>(applicationContext, iteration, lastRepeatContext.timeoutInstant, nextBackoff, companionValue);

			if (!repeatPredicate.test(repeatContext)) {
				log.debug("Stopping repeats since predicate returned false, retry context: {}", repeatContext);
				return RETRY_EXHAUSTED;
			}
			else if (nextBackoff == RETRY_EXHAUSTED) {
				log.debug("Repeats exhausted, retry context: {}", repeatContext);
				return RETRY_EXHAUSTED;
			}
			else {
				log.debug("Scheduling repeat attempt, retry context: {}", repeatContext);
				this.lastRepeatContext = repeatContext;
				onRepeat.accept(repeatContext);
				return nextBackoff;
			}
		}
	}
}
