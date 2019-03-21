/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.function.Supplier;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

/**
 * Opinionated caching helper that defines how to store and restore a {@link Mono} in an
 * arbitrary cache abstraction. A generic writer/reader interface is provided, but cache
 * vendors that have a Map wrapper support can also be directly used.
 * <p>
 * Example usage:
 * <pre><code>
 *    LoadingCache<Integer, Object> graphs = Caffeine.newBuilder()
 *                                       .maximumSize(10_000)
 *                                       .expireAfterWrite(5, TimeUnit.MINUTES)
 *                                       .refreshAfterWrite(1, TimeUnit.MINUTES)
 *                                       .build(key -> createExpensiveGraph(key));
 *
 *    keyStream.concatMap(key -> CacheMono.lookup(graphs.asMap(), key)
 *                                    .onCacheMissResume(repository.findOneById(key))
 * </code></pre>
 * </p>
 *
 * @author Oleh Dokuka
 * @author Simon Baslé
 */
public class CacheMono {

	/**
	 * Restore a {@link Mono Mono&lt;VALUE&gt;} from the cache-map given a provided key. If no value
	 * is in the cache, it will be calculated from the original source which is set up in
	 * the next step. Note that if the source completes empty, this result will be cached
	 * and all subsequent requests with the same key will return {@link Mono#empty()}. The
	 * behaviour is similar for erroring sources, except cache hits would then return
	 * {@link Mono#error(Throwable)}.
	 * <p>
	 * Note that the wrapped {@link Mono} is lazy, meaning that subscribing twice in a row
	 * to the returned {@link Mono} on an empty cache will trigger a cache miss then a
	 * cache hit.
	 *
	 * @param cacheMap {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link MonoCacheBuilderMapMiss builder step} to use to set up the source
	 */
	public static <KEY, VALUE> MonoCacheBuilderMapMiss<VALUE> lookup(Map<KEY, ? super Signal<? extends VALUE>> cacheMap, KEY key) {
		return otherSupplier -> Mono.defer(() ->
				Mono.justOrEmpty(cacheMap.get(key))
				    .switchIfEmpty(otherSupplier.get().materialize()
				                    .doOnNext(value -> cacheMap.put(key, value)))
				    .dematerialize()
		);
	}

	/**
	 * Restore a {@link Mono Mono&lt;VALUE&gt;} from the {@link MonoCacheReader} given a provided
	 * key. If no value is in the cache, it will be calculated from the original source
	 * which is set up in the next step. Note that if the source completes empty, this
	 * result will be cached and all subsequent requests with the same key will return
	 * {@link Mono#empty()}. The behaviour is similar for erroring sources, except cache
	 * hits would then return {@link Mono#error(Throwable)}.
	 * <p>
	 * Note that the wrapped {@link Mono} is lazy, meaning that subscribing twice in a row
	 * to the returned {@link Mono} on an empty cache will trigger a cache miss then a
	 * cache hit.
	 *
	 * @param reader a {@link MonoCacheReader} function that looks up {@link Signal} from a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link MonoCacheBuilderCacheMiss builder step} to use to set up the source
	 */
	public static <KEY, VALUE> MonoCacheBuilderCacheMiss<KEY, VALUE> lookup(MonoCacheReader<KEY, VALUE> reader, KEY key) {
		return otherSupplier -> writer -> Mono.defer(() ->
				reader.apply(key)
				  .switchIfEmpty(otherSupplier.get()
				                              .materialize()
				                              .flatMap(signal -> writer.apply(key, signal)
				                                                       .then(Mono.just(signal))
				                              )
				  )
				  .dematerialize());
	}

	// ==== Support interfaces ====

	/**
	 * Functional interface that gives ability to lookup for cached result from the Cache
	 * source. <p> Example adapter around {@link Map} usage:
	 * <pre><code>
	 * Map<Integer, Signal<? extends String>> cache = new HashMap<>();
	 * Function<Integer, Mono<String>> flatMap = key -> CacheMono
	 *                                                   .lookup(reader(cache), key)
	 *                                                   .onCacheMissResume(source)
	 *                                                   .andWriteWith(writer(cache));
	 *
	 *
	 * private static <Key, Value> CacheMono.MonoCacheReader<Key, Value> reader(Map<Key, ? extends Signal<? extends Value>> cache) {
	 *    return key -> Mono.justOrEmpty(cache.get(key));
	 * }
	 *
	 * private static <Key, Value> CacheMono.MonoCacheWriter<Key, Value> writer(Map<Key, ? super Signal<? extends Value>> cache) {
	 *    return (key, value) -> {
	 *        cache.put(key, value);
	 *        return Mono.just(value);
	 *    };
	 * }
	 * </code></pre>
	 * </p>
	 *
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 */
	@FunctionalInterface
	interface MonoCacheReader<KEY, VALUE>
			extends Function<KEY, Mono<Signal<? extends VALUE>>> {

	}

	/**
	 * Functional interface that gives ability to write results from the original source
	 * to Cache-storage. The returned {@link Mono Mono&lt;Void&gt;} represents the
	 * completion of the cache write.
	 *
	 * <p> Example adapter around {@link Map} usage:
	 * <pre><code>
	 * Map<Integer, Signal<? extends String>> cache = new HashMap<>();
	 * Function<Integer, Mono<String>> flatMap = key -> CacheMono
	 *                                                   .lookup(reader(cache), key)
	 *                                                   .onCacheMissResume(source)
	 *                                                   .andWriteWith(writer(cache));
	 *
	 *
	 * private static <Key, Value> CacheMono.MonoCacheReader<Key, Value> reader(Map<Key, ? extends Signal<? extends Value>> cache) {
	 *    return key -> Mono.justOrEmpty(cache.get(key));
	 * }
	 *
	 * private static <Key, Value> CacheMono.MonoCacheWriter<Key, Value> writer(Map<Key, ? super Signal<? extends Value>> cache) {
	 *    return (key, value) -> Mono.fromRunnable(() -> cache.put(key, value));
	 * }
	 * </code></pre>
	 * </p>
	 *
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 */
	@FunctionalInterface
	interface MonoCacheWriter<KEY, VALUE> extends
	                                      BiFunction<KEY, Signal<? extends VALUE>, Mono<Void>> {

	}

	// ==== Mono Builders ====

	/**
	 * Setup original source to fallback to in case of cache miss.
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	interface MonoCacheBuilderCacheMiss<KEY, VALUE> {

		/**
		 * Setup original source to fallback to in case of cache miss.
		 *
		 * @param other original source
		 *
		 * @return the {@link MonoCacheBuilderCacheWriter next step} of the builder
		 */
		default MonoCacheBuilderCacheWriter<KEY, VALUE> onCacheMissResume(
				Mono<VALUE> other) {
			return onCacheMissResume(() -> other);
		}

		/**
		 * Setup original source to fallback to in case of cache miss, through a
		 * {@link Supplier} which allows lazy resolution of the source.
		 *
		 * @param otherSupplier original source {@link Supplier}
		 *
		 * @return the {@link MonoCacheBuilderCacheWriter next step} of the builder
		 */
		MonoCacheBuilderCacheWriter<KEY, VALUE> onCacheMissResume(Supplier<Mono<VALUE>> otherSupplier);
	}

	/**
	 * Set up the {@link MonoCacheWriter} to use to store the source data into the cache
	 * in case of cache miss.
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	interface MonoCacheBuilderCacheWriter<KEY, VALUE> {

		/**
		 * Set up the {@link MonoCacheWriter} to use to store the source data into the cache
		 * in case of cache miss.
		 *
		 * @param writer {@link MonoCacheWriter} instance
		 *
		 * @return A wrapped {@link Mono} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		Mono<VALUE> andWriteWith(MonoCacheWriter<? super KEY, VALUE> writer);
	}

	/**
	 * Setup original source to fallback to in case of cache miss and return a wrapped
	 * {@link Mono} that transparently looks up data from a {@link Map} representation of
	 * a cache and store data into the cache in case of cache miss.
	 *
	 * @param <VALUE> type
	 */
	interface MonoCacheBuilderMapMiss<VALUE> {

		/**
		 * Setup original source to fallback to in case of cache miss.
		 *
		 * @param other original source
		 *
		 * @return A wrapped {@link Mono} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		default Mono<VALUE> onCacheMissResume(Mono<VALUE> other) {
			return onCacheMissResume(() -> other);
		}

		/**
		 * Setup original source to fallback to in case of cache miss, through a
		 * {@link Supplier} which allows lazy resolution of the source.
		 *
		 * @param otherSupplier original source {@link Supplier}
		 *
		 * @return A wrapped {@link Mono} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		Mono<VALUE> onCacheMissResume(Supplier<Mono<VALUE>> otherSupplier);
	}

}
