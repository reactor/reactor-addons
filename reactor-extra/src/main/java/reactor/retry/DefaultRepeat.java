/*
 * Copyright (c) 2017-2025 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

/*
 * @deprecated Use reactor.util.repeat.RepeatSpec available since reactor-core 3.8.0
 * which provides similar capabilities.
 */
@Deprecated
public class DefaultRepeat<T> extends AbstractRetry<T, Long> implements Repeat<T> {

	static final Logger log = Loggers.getLogger(DefaultRepeat.class);
	static final Consumer<? super RepeatContext<?>> NOOP_ON_REPEAT = r -> {};

	final Predicate<? super RepeatContext<T>> repeatPredicate;
	final Consumer<? super RepeatContext<T>> onRepeat;

	DefaultRepeat(Predicate<? super RepeatContext<T>> repeatPredicate,
			long maxRepeats,
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

	public static <T> DefaultRepeat<T> create(Predicate<? super RepeatContext<T>> repeatPredicate, long n) {
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
		return new DefaultRepeat<>(repeatPredicate, maxIterations, timeout,
				backoff, jitter, backoffScheduler, onRepeat, applicationContext);
	}

	@Override
	public Repeat<T> repeatMax(long maxRepeats) {
		if (maxRepeats < 1)
			throw new IllegalArgumentException("maxRepeats should be > 0");
		return new DefaultRepeat<>(repeatPredicate, maxRepeats, timeout, backoff, jitter,
				backoffScheduler, onRepeat, applicationContext);
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

	@Override
	public Publisher<Long> apply(Flux<Long> companionValues) {
		Instant timeoutInstant = calculateTimeout();
		DefaultContext<T> context = new DefaultContext<>(applicationContext, 0, null, -1L);
		return companionValues
				.index()
				.map(tuple -> repeatBackoff(tuple.getT2(), tuple.getT1() + 1L, timeoutInstant, context))
				.takeWhile(backoff -> backoff != RETRY_EXHAUSTED)
				.concatMap(backoff -> retryMono(backoff.delay));
	}

	BackoffDelay repeatBackoff(Long companionValue, Long iteration, Instant timeoutInstant, DefaultContext<T> context) {
		DefaultContext<T> tmpContext = new DefaultContext<>(applicationContext, iteration, context.lastBackoff, companionValue);
		BackoffDelay nextBackoff = calculateBackoff(tmpContext, timeoutInstant);
		DefaultContext<T> repeatContext = new DefaultContext<>(applicationContext, iteration, nextBackoff, companionValue);
		context.lastBackoff = nextBackoff;

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
			onRepeat.accept(repeatContext);
			return nextBackoff;
		}
	}

	@Override
	public String toString() {
		return "Repeat{times=" + this.maxIterations + ",backoff=" + backoff + ",jitter=" +
				jitter + "}";
	}
}

