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


public interface Retry<T> extends Function<Flux<Throwable>, Publisher<Long>> {

	static <T> Retry<T> any() {
		return DefaultRetry.<T>create(context -> true);
	}

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

	static <T> Retry<T> onlyIf(Predicate<? super RetryContext<T>> predicate) {
		return DefaultRetry.create(predicate).retryMax(Integer.MAX_VALUE);
	}

	Retry<T> withApplicationContext(T applicationContext);

	Retry<T> doOnRetry(Consumer<? super RetryContext<T>> onRetry);

	default Retry<T> retryOnce() {
		return retryMax(1);
	}

	Retry<T> retryMax(int maxRetries);

	Retry<T> timeout(Duration timeout);

	Retry<T> backoff(Backoff backoff);

	Retry<T> jitter(Jitter jitter);

	Retry<T> withBackoffScheduler(Scheduler scheduler);

	default Retry<T> noBackoff() {
		return backoff(Backoff.zero());
	}

	default Retry<T> fixedBackoff(Duration backoffInterval) {
		return backoff(Backoff.fixed(backoffInterval));
	}

	default Retry<T> exponentialBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff));
	}

	default Retry<T> exponentialBackoffWithJitter(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff)).jitter(Jitter.random());
	}

	default Retry<T> randomBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.prevTimes3(firstBackoff, maxBackoff)).jitter(Jitter.random());
	}

	default <S> Flux<S> apply(Publisher<S> source) {
		return Flux.from(source).retryWhen(this);
	}

}
