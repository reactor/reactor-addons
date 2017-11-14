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

public class CacheHelper {

	/**
	 * Restore a {@link Mono<Value>} from the cache-map given a provided key. If no value
	 * is in the cache, it will be calculated from the original source which is set up in
	 * the next step. Note that if the source completes empty, this result will be cached
	 * and all subsequent requests with the same key will return {@link Mono#empty()). The
	 * behaviour is similar for erroring sources, except cache hits would then return
	 * {@link Mono#error(Throwable)}.
	 *
	 * @param cache {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <Key> Key Type
	 * @param <Value> Value Type
	 *
	 * @return Lazy "Or" Builder
	 */
	@NonNull
	public static <Key, Value> Or<Value> hydrateFrom(@NonNull Map<Key, Signal<? extends Value>> cache,
			@NonNull Key key) {
		return other -> {
			Signal<? extends Value> cached = cache.get(key);

			if (cached == null) {
				return other.materialize()
				            .doOnNext(value -> cache.put(key, value))
				            .dematerialize();
			}
			else {
				return Mono.just(cached)
				           .dematerialize();
			}
		};
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
}
