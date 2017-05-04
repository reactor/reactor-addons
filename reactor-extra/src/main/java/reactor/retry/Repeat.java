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

public interface Repeat<T> extends Function<Flux<Long>, Publisher<Long>> {

	static <T> Repeat<T> onlyIf(Predicate<? super RepeatContext<T>> predicate) {
		return DefaultRepeat.create(predicate, Integer.MAX_VALUE);
	}

	static <T> Repeat<T> once() {
		return times(1);
	}

	static <T> Repeat<T> times(int n) {
		if (n < 0)
			throw new IllegalArgumentException("n should be >= 0");
		return DefaultRepeat.create(context -> true, n);
	}

	Repeat<T> withApplicationContext(T applicationContext);

	Repeat<T> doOnRepeat(Consumer<? super RepeatContext<T>> onRepeat);

	Repeat<T> timeout(Duration timeout);

	Repeat<T> backoff(Backoff backoff);

	Repeat<T> jitter(Jitter jitter);

	Repeat<T> withBackoffScheduler(Scheduler scheduler);

	default Repeat<T> noBackoff() {
		return backoff(Backoff.zero());
	}

	default Repeat<T> fixedBackoff(Duration backoffInterval) {
		return backoff(Backoff.fixed(backoffInterval));
	}

	default Repeat<T> exponentialBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 2, false));
	}

	default Repeat<T> exponentialBackoffWithJitter(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 2, false)).jitter(Jitter.random());
	}

	default Repeat<T> randomBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 3, true)).jitter(Jitter.random());
	}

	default <S> Flux<S> apply(Publisher<S> source) {
		return Flux.from(source).repeatWhen(this);
	}

}
