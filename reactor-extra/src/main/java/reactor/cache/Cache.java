/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.cache;

import java.util.Map;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.util.annotation.NonNull;

/**
 * Cache helper that may be used with any cache vendors that has a Map wrapper support.
 * <p>
 * Example usage:
 * <pre><code>
 *    LoadingCache<Integer, Object> graphs = Caffeine.newBuilder()
 *                                       .maximumSize(10_000)
 *                                       .expireAfterWrite(5, TimeUnit.MINUTES)
 *                                       .refreshAfterWrite(1, TimeUnit.MINUTES)
 *                                       .build(key -> createExpensiveGraph(key));
 *
 *    keyStream.concatMap(key -> Cache.hydrateFrom(graphs.asMap(), key)
 *                                   .or(repository.findOneById(key))
 * </code></pre>
 *
 * @author Oleh Dokuka
 */

public class Cache {

	private static final Object NULL_HOLDER = new Object();

	/**
	 * Current method may be used to drain the value from the cache-map by given key and
	 * transform the result to {@link Mono<Value>}. In the case of empty value the value
	 * will be calculated from the alternative source. Note, if the alternative source
	 * returns empty result, it will be consider as the result and for all subsequent
	 * requests with the identical key will be returned {@link Mono#empty()). The similar
	 * behaviours is for the case when alternative source completed with error, thus for
	 * all subsequent identical keys will be emitted {@link Mono#error(Throwable)}
	 *
	 * @param cache {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <Key> Key Type
	 * @param <Value> Value Type
	 *
	 * @return Lazy "Or" Builder
	 */
	@NonNull
	public static <Key, Value> Or<Value> hydrateFrom(@NonNull Map<Key, Value> cache,
			@NonNull Key key) {
		return other -> {
			Value cached = cache.get(key);

			if (cached == null) {
				return other.materialize()
				            .doOnNext(value -> cache.put(key, get(value)))
				            .dematerialize();
			}
			else if (cached == NULL_HOLDER) {
				return Mono.empty();
			}
			else if (cached instanceof ErrorHolder) {
				return Mono.error(((ErrorHolder) cached).e);
			}
			else {
				return Mono.just(cached);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private static <Value> Value get(Signal<? extends Value> signal) {
		Value value = signal.get();

		return signal.hasError() ? (Value) new ErrorHolder(signal.getThrowable()) :
				value == null ? (Value) NULL_HOLDER : value;
	}

	public interface Or<Value> {

		/**
		 * Fallback {@link Mono<Value>} in case of missing value in cache
		 *
		 * @param other fallback
		 *
		 * @return Mono with value
		 */
		@NonNull
		Mono<Value> or(Mono<? extends Value> other);
	}

	private static class ErrorHolder {

		private final Throwable e;

		private ErrorHolder(Throwable e) {
			this.e = e;
		}
	}
}
