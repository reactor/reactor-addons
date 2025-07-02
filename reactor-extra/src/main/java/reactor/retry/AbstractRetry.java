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
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.scheduler.clock.SchedulerClock;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

/*
 * @deprecated Use reactor.util.repeat or reactor.util.retry available since
 * reactor-core 3.8.0 which provides similar capabilities.
 */
@Deprecated
public abstract class AbstractRetry<T, S> implements Function<Flux<S>, Publisher<Long>> {

	static final Logger log = Loggers.getLogger(AbstractRetry.class);

	static final BackoffDelay RETRY_EXHAUSTED = new BackoffDelay(Duration.ofSeconds(-1)) {
		@Override
		public String toString() {
			return "{EXHAUSTED}";
		}
	};

	final long maxIterations;
	final Duration timeout;
	final Backoff backoff;
	final Jitter jitter;
	@Nullable
	final Scheduler backoffScheduler;
	final SchedulerClock clock;
	final T applicationContext;

	AbstractRetry(long maxIterations,
			Duration timeout,
			Backoff backoff,
			Jitter jitter,
			@Nullable
			Scheduler backoffScheduler,
			T applicationContext) {
		this.maxIterations = maxIterations;
		this.timeout = timeout;
		this.backoff = backoff;
		this.jitter = jitter;
		this.backoffScheduler = backoffScheduler;
		this.clock = SchedulerClock.of(backoffScheduler == null ? Schedulers.parallel() : backoffScheduler);
		this.applicationContext = applicationContext;
	}

	Instant calculateTimeout() {
		return timeout != null ? Instant.now(clock).plus(timeout) : Instant.MAX;
	}

	BackoffDelay calculateBackoff(IterationContext<T> retryContext, Instant timeoutInstant) {
		if (retryContext.iteration() > maxIterations)
			return RETRY_EXHAUSTED;

		BackoffDelay nextBackoff = backoff.apply(retryContext);
		Duration minBackoff = nextBackoff.min;
		Duration maxBackoff = nextBackoff.max;
		Duration backoff = nextBackoff.delay;
		if (maxBackoff != null)
			backoff = backoff.compareTo(maxBackoff) < 0 ? backoff : maxBackoff;
		if (minBackoff != null)
			backoff = backoff.compareTo(minBackoff) > 0 ? backoff : minBackoff;

		BackoffDelay sanitizedBackoff = new BackoffDelay(minBackoff, maxBackoff, backoff);
		Duration jitteredBackoff = jitter.apply(sanitizedBackoff);

		if (Instant.now(clock).plus(jitteredBackoff).isAfter(timeoutInstant))
			return RETRY_EXHAUSTED;
		else
			return new BackoffDelay(minBackoff, maxBackoff, jitteredBackoff);
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
