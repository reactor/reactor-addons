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
import java.util.function.BiFunction;
import java.util.function.Function;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.util.annotation.NonNull;

/**
 * Cache helper that may be used with any cache vendors that has a Map wrapper support.
 * <p> Example usage:
 * <pre><code>
 *    LoadingCache<Integer, Object> graphs = Caffeine.newBuilder()
 *                                       .maximumSize(10_000)
 *                                       .expireAfterWrite(5, TimeUnit.MINUTES)
 *                                       .refreshAfterWrite(1, TimeUnit.MINUTES)
 *                                       .build(key -> createExpensiveGraph(key));
 *
 *    keyStream.concatMap(key -> Cache.lookupMono(graphs.asMap(), key)
 *                                    .onCacheMissResume(repository.findOneById(key))
 * </code></pre>
 * </p>
 *
 * @author Oleh Dokuka
 * @author Simon Baslé
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
	 * @return Lazy "{@link MonoCacheBuilderMapMiss}"
	 */
	@NonNull
	public static <Key, Value> MonoCacheBuilderMapMiss<Value> lookupMono(@NonNull Map<Key, ? super Signal<? extends Value>> cm,
			@NonNull Key key) {
		return o -> Mono.justOrEmpty(cm.get(key))
		                .switchIfEmpty(o.materialize()
		                                .doOnNext(value -> cm.put(key, value)))
		                .dematerialize();
	}

	/**
	 * Restore a {@link Mono<Value>} from the {@link MonoCacheReader} given a provided
	 * key. If no value is in the cache, it will be calculated from the original source
	 * which is set up in the next step. Note that if the source completes empty, this
	 * result will be cached and all subsequent requests with the same key will return
	 * {@link Mono#empty()). The behaviour is similar for erroring sources, except cache
	 * hits would then return {@link Mono#error(Throwable)}.
	 *
	 * @param cache {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <Key> Key Type
	 * @param <Value> Value Type
	 *
	 * @return Lazy "{@link MonoCacheBuilderCacheMiss}"
	 */
	@NonNull
	public static <Key, Value> MonoCacheBuilderCacheMiss<Key, Value> lookupMono(@NonNull MonoCacheReader<Key, Value> cr,
			@NonNull Key key) {
		return o -> cw -> cr.apply(key)
		                    .switchIfEmpty(o.materialize()
		                                    .flatMap(value -> cw.apply(key, value)))
		                    .dematerialize();

	}

	/**
	 * Functional interface that gives ability to lookup for cached result from the Cache
	 * source. <p> Example adapter around {@link Map} usage:
	 * <pre><code>
	 * Map<Integer, Signal<? extends String>> cache = new HashMap<>();
	 * Function<Integer, Mono<String>> flatMap = key -> CacheHelper
	 *                                                   .lookupMono(reader(cache), key)
	 *                                                   .onCacheMissResume(source)
	 *                                                   .andWriteWith(writer(cache));
	 *
	 *
	 * private static <Key, Value> CacheHelper.MonoCacheReader<Key, Value> reader(Map<Key, ? extends Signal<? extends Value>> cache) {
	 *    return key -> Mono.justOrEmpty(cache.get(key));
	 * }
	 *
	 * private static <Key, Value> CacheHelper.MonoCacheWriter<Key, Value> writer(Map<Key, ? super Signal<? extends Value>> cache) {
	 *    return (key, value) -> {
	 *        cache.put(key, value);
	 *        return Mono.just(value);
	 *    };
	 * }
	 * </code></pre>
	 * </p>
	 *
	 * @param <Key> Key Type
	 * @param <Value> Value Type
	 */
	@FunctionalInterface
	interface MonoCacheReader<Key, Value>
			extends Function<Key, Mono<Signal<? extends Value>>> {

	}

	/**
	 * Functional interface that gives ability to write results from the original source
	 * to Cache-storage.
	 * <p> Example adapter around {@link Map} usage:
	 * <pre><code>
	 * Map<Integer, Signal<? extends String>> cache = new HashMap<>();
	 * Function<Integer, Mono<String>> flatMap = key -> CacheHelper
	 *                                                   .lookupMono(reader(cache), key)
	 *                                                   .onCacheMissResume(source)
	 *                                                   .andWriteWith(writer(cache));
	 *
	 *
	 * private static <Key, Value> CacheHelper.MonoCacheReader<Key, Value> reader(Map<Key, ? extends Signal<? extends Value>> cache) {
	 *    return key -> Mono.justOrEmpty(cache.get(key));
	 * }
	 *
	 * private static <Key, Value> CacheHelper.MonoCacheWriter<Key, Value> writer(Map<Key, ? super Signal<? extends Value>> cache) {
	 *    return (key, value) -> {
	 *        cache.put(key, value);
	 *        return Mono.just(value);
	 *    };
	 * }
	 * </code></pre>
	 * </p>
	 *
	 * @param <Key> Key Type
	 * @param <Value> Value Type
	 */
	@FunctionalInterface
	interface MonoCacheWriter<Key, Value> extends
	                                      BiFunction<Key, Signal<? extends Value>, Mono<Signal<? extends Value>>> {

	}

	/**
	 * Builder that setup original source
	 *
	 * @param <Key> Key type
	 * @param <Value> Value type
	 */
	@FunctionalInterface
	interface MonoCacheBuilderCacheMiss<Key, Value> {

		/**
		 * Setup original source
		 *
		 * @param other original source
		 *
		 * @return lazy {@link MonoCacheBuilderCacheWriter}
		 */
		MonoCacheBuilderCacheWriter<Key, Value> onCacheMissResume(Mono<? extends Value> other);
	}

	/**
	 * Builder that setup {@link MonoCacheWriter}
	 *
	 * @param <Key> Key type
	 * @param <Value> Value type
	 */
	@FunctionalInterface
	interface MonoCacheBuilderCacheWriter<Key, Value> {

		/**
		 * Setup {@link MonoCacheWriter}
		 *
		 * @param writer {@link MonoCacheWriter} instance
		 *
		 * @return {@link Mono}
		 */
		Mono<Value> andWriteWith(MonoCacheWriter<? super Key, Value> writer);
	}

	/**
	 * Cache builder that adapt {@link Map} as the Cache source and skip building steps
	 * such as specified by {@link MonoCacheBuilderCacheWriter}
	 *
	 * <p> Example usage:
	 * <pre><code>
	 *    LoadingCache<Integer, Object> graphs = Caffeine.newBuilder()
	 *                                       .maximumSize(10_000)
	 *                                       .expireAfterWrite(5, TimeUnit.MINUTES)
	 *                                       .refreshAfterWrite(1, TimeUnit.MINUTES)
	 *                                       .build(key -> createExpensiveGraph(key));
	 *
	 *    keyStream.concatMap(key -> Cache.lookupMono(graphs.asMap(), key)
	 *                                    .onCacheMissResume(repository.findOneById(key))
	 * </code></pre>
	 * </p>
	 *
	 * @param <Value> type
	 */
	@FunctionalInterface
	interface MonoCacheBuilderMapMiss<Value> {

		/**
		 * Setup original source
		 *
		 * @param other original source
		 *
		 * @return direct {@link Mono}
		 */
		Mono<Value> onCacheMissResume(Mono<? extends Value> other);
	}
}
