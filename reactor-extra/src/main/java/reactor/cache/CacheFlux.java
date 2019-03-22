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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

/**
 * Opinionated caching helper that defines how to store and restore a {@link Flux} in an
 * arbitrary cache abstraction. A generic writer/reader interface is provided, but cache
 * vendors that have a Map wrapper support can also be directly used.
 * <p>
 * <pre><code>
 *    LoadingCache<Integer, Object> graphs = Caffeine.newBuilder()
 *                                       .maximumSize(10_000)
 *                                       .expireAfterWrite(5, TimeUnit.MINUTES)
 *                                       .refreshAfterWrite(1, TimeUnit.MINUTES)
 *                                       .build(key -> createExpensiveGraph(key));
 *
 *    keyStream.concatMap(key -> CacheFlux.lookup(graphs.asMap(), key)
 *                                    .onCacheMissResume(repository.findOneById(key))
 * </code></pre>
 * </p>
 *
 * @author Oleh Dokuka
 * @author Simon Baslé
 */
public class CacheFlux {


	/**
	 * Restore a {@link Flux Flux&lt;VALUE&gt;} from the cache-map given a provided key.
	 * The cache is expected to store original values as a {@link List} of {@link Signal}
	 * of T. If no value is in the cache, it will be calculated from the original source
	 * which is set up in the next step. Note that if the source completes empty, this
	 * result will be cached and all subsequent requests with the same key will return
	 * {@link Flux#empty()}. The behaviour is similar for erroring sources, except cache
	 * hits would then return {@link Flux#error(Throwable)}.
	 * <p>
	 * Note that the wrapped {@link Flux} is lazy, meaning that subscribing twice in a row
	 * to the returned {@link Flux} on an empty cache will trigger a cache miss then a
	 * cache hit.
	 *
	 * @param cacheMap {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link FluxCacheBuilderMapMiss builder step} to use to set up the source
	 */
	public static <KEY, VALUE> FluxCacheBuilderMapMiss<VALUE> lookup(
			Map<KEY, ? super List> cacheMap, KEY key, Class<VALUE> valueClass) {
		return otherSupplier ->
				Flux.defer(() -> {
					Object fromCache = cacheMap.get(key);
					if (fromCache == null) {
						return otherSupplier.get()
						                    .materialize()
						                    .collectList()
						                    .doOnNext(signals -> cacheMap.put(key, signals))
						                    .flatMapIterable(Function.identity())
						                    .dematerialize();
					}
					else if (fromCache instanceof List) {
						try {
							@SuppressWarnings("unchecked")
							List<Signal<VALUE>> fromCacheSignals = (List<Signal<VALUE>>) fromCache;
							return Flux.fromIterable(fromCacheSignals)
							           .dematerialize();
						}
						catch (Throwable cause) {
							return Flux.error(new IllegalArgumentException("Content of cache for key " + key + " cannot be cast to List<Signal>", cause));
						}
					}
					else {
						return Flux.error(new IllegalArgumentException("Content of cache for key " + key + " is not a List"));
					}
				});
	}

	/**
	 * Restore a {@link Flux Flux&lt;VALUE&gt;} from the {@link FluxCacheReader} given a provided
	 * key. The cache is expected to store original values as a {@link List} of {@link Signal}
	 * of T. If no value is in the cache, it will be calculated from the original source
	 * which is set up in the next step. Note that if the source completes empty, this
	 * result will be cached and all subsequent requests with the same key will return
	 * {@link Flux#empty()}. The behaviour is similar for erroring sources, except cache
	 * hits would then return {@link Flux#error(Throwable)}.
	 * <p>
	 * Note that the wrapped {@link Flux} is lazy, meaning that subscribing twice in a row
	 * to the returned {@link Flux} on an empty cache will trigger a cache miss then a
	 * cache hit.
	 *
	 * @param reader a {@link FluxCacheReader} function that looks up collection of {@link Signal} from a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link FluxCacheBuilderCacheMiss builder step} used to set up the source
	 */
	public static <KEY, VALUE> FluxCacheBuilderCacheMiss<KEY, VALUE> lookup(FluxCacheReader<KEY, VALUE> reader, KEY key) {
		return otherSupplier -> writer ->
				Flux.defer(() ->
						reader.apply(key)
						  .switchIfEmpty(otherSupplier.get()
						                              .materialize()
						                              .collectList()
						                              .flatMap(signals -> writer.apply(key, signals)
						                                                        .then(Mono.just(signals))))
						  .flatMapIterable(Function.identity())
						  .dematerialize()
		);
	}

	// ==== Support interfaces ====

	/**
	 * Functional interface that gives ability to lookup for cached multiple results from the Cache
	 * source.
	 *
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 */
	@FunctionalInterface
	interface FluxCacheReader<KEY, VALUE>
			extends Function<KEY, Mono<List<Signal<VALUE>>>> {

	}

	/**
	 * Functional interface that gives ability to write results from a {@link Flux} source
	 * to Cache-storage, as a {@code List<Signal<T>>}. The bifunction must return a {@link Mono}
	 * representing the fact that the list of signals has been stored, without modification.
	 *
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type of the source. The source is stored as a {@code List<Signal<VALUE>>}
	 */
	@FunctionalInterface
	interface FluxCacheWriter<KEY, VALUE> extends
	                                      BiFunction<KEY, List<Signal<VALUE>>, Mono<Void>> {

	}

	// ==== Flux Builders ====

	/**
	 * Setup original source to fallback to in case of cache miss.
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	interface FluxCacheBuilderCacheMiss<KEY, VALUE> {

		/**
		 * Setup original source to fallback to in case of cache miss.
		 *
		 * @param other original source
		 *
		 * @return The {@link FluxCacheBuilderCacheWriter next step} of the builder
		 */
		default FluxCacheBuilderCacheWriter<KEY, VALUE> onCacheMissResume(Flux<VALUE> other) {
			return onCacheMissResume(() -> other);
		}

		/**
		 * Setup original source to fallback to in case of cache miss, through a
		 * {@link Supplier} which allows lazy resolution of the source.
		 *
		 * @param otherSupplier original source {@link Supplier}
		 *
		 * @return The {@link FluxCacheBuilderCacheWriter next step} of the builder
		 */
		FluxCacheBuilderCacheWriter<KEY, VALUE> onCacheMissResume(Supplier<Flux<VALUE>> otherSupplier);
	}

	/**
	 * Set up the {@link FluxCacheWriter} to use to store the source data into the cache
	 * in case of cache miss.
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	interface FluxCacheBuilderCacheWriter<KEY, VALUE> {

		/**
		 * Set up the {@link FluxCacheWriter} to use to store the source data into the cache
		 * in case of cache miss.
		 *
		 * @param writer {@link FluxCacheWriter} instance
		 *
		 * @return A wrapped {@link Flux} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		Flux<VALUE> andWriteWith(FluxCacheWriter<? super KEY, VALUE> writer);
	}

	/**
	 * Setup original source to fallback to in case of cache miss and return a wrapped
	 * {@link Flux} that transparently looks up data from a {@link Map} representation of
	 * a cache and store data into the cache in case of cache miss.
	 *
	 * @param <VALUE> type
	 */
	interface FluxCacheBuilderMapMiss<VALUE> {

		/**
		 * Setup original source to fallback to in case of cache miss.
		 *
		 * @param other original source
		 *
		 * @return A wrapped {@link Flux} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		default Flux<VALUE> onCacheMissResume(Flux<VALUE> other) {
			return onCacheMissResume(() -> other);
		}

		/**
		 * Setup original source to fallback to in case of cache miss, through a
		 * {@link Supplier} which allows lazy resolution of the source.
		 *
		 * @param otherSupplier original source {@link Supplier}
		 *
		 * @return A wrapped {@link Flux} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		Flux<VALUE> onCacheMissResume(Supplier<Flux<VALUE>> otherSupplier);
	}

}
