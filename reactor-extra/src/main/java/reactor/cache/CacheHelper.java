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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

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
	 * @param cm {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return Lazy "{@link MonoCacheBuilderMapMiss}"
	 */
	public static <KEY, VALUE> MonoCacheBuilderMapMiss<VALUE> lookupMono(Map<KEY, ? super Signal<? extends VALUE>> cm, KEY key) {
		return o -> Mono.defer(() ->
				Mono.justOrEmpty(cm.get(key))
				    .switchIfEmpty(o.materialize()
				                    .doOnNext(value -> cm.put(key, value)))
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
	 * @param cr a {@link MonoCacheReader} function that looks up {@link Signal} from a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return Lazy "{@link MonoCacheBuilderCacheMiss}"
	 */
	public static <KEY, VALUE> MonoCacheBuilderCacheMiss<KEY, VALUE> lookupMono(MonoCacheReader<KEY, VALUE> cr, KEY key) {
		return o -> cw -> Mono.defer(() -> cr.apply(key)
		                    .switchIfEmpty(o.materialize()
		                                    .flatMap(value -> cw.apply(key, value)))
		                    .dematerialize()
		);
	}

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
	 * @param cm {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return Lazy "{@link MonoCacheBuilderMapMiss}"
	 */
	public static <KEY, VALUE> FluxCacheBuilderMapMiss<VALUE> lookupFlux(Map<KEY, ? super List<? super Signal<VALUE>>> cm, KEY key) {
		return other -> Flux.defer(() -> {
			Object fromCache = cm.get(key);
			if (fromCache == null) {
				return other.materialize()
				            .cast(Object.class)
				            .collectList()
				            .doOnNext(signals -> cm.put(key, signals))
				            .flatMapIterable(Function.identity())
				            .dematerialize();
			}
			else if (fromCache instanceof Iterable) {
				try {
					@SuppressWarnings("unchecked")
					Iterable<Signal<VALUE>> fromCacheSignals = (Iterable<Signal<VALUE>>) fromCache;
					return Flux.fromIterable(fromCacheSignals)
					           .dematerialize();
				}
				catch (Throwable cause) {
					return Flux.error(new IllegalArgumentException("Content of cache for key " + key + " cannot be cast to Iterable<Signal>", cause));
				}
			}
			else {
				return Flux.error(new IllegalArgumentException("Content of cache for key " + key + " is not an Iterable"));
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
	 * @param cr a {@link FluxCacheReader} function that looks up collection of {@link Signal} from a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return Lazy "{@link FluxCacheBuilderCacheMiss}"
	 */
	public static <KEY, VALUE> FluxCacheBuilderCacheMiss<KEY, VALUE> lookupFlux(FluxCacheReader<KEY, VALUE> cr, KEY key) {
		return other -> writer -> Flux.defer(() ->
				cr.apply(key)
				  .switchIfEmpty(other.materialize()
				                      .collectList()
				                      .flatMap(signals -> writer.apply(key, signals)))
				  .flatMapIterable(Function.identity())
				  .dematerialize()
		);
	}

	// ==== Support interfaces ====

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
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 */
	@FunctionalInterface
	interface MonoCacheReader<KEY, VALUE>
			extends Function<KEY, Mono<Signal<? extends VALUE>>> {

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
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 */
	@FunctionalInterface
	interface MonoCacheWriter<KEY, VALUE> extends
	                                      BiFunction<KEY, Signal<? extends VALUE>, Mono<Signal<? extends VALUE>>> {

	}

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
	 * to Cache-storage, as a {@code List<Signal<T>>}.
	 *
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 */
	@FunctionalInterface
	interface FluxCacheWriter<KEY, VALUE> extends
	                                      BiFunction<KEY, List<Signal<VALUE>>, Mono<List<Signal<VALUE>>>> {

	}

	// ==== Mono Builders ====

	/**
	 * Builder that setup original source
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	@FunctionalInterface
	interface MonoCacheBuilderCacheMiss<KEY, VALUE> {

		/**
		 * Setup original source
		 *
		 * @param other original source
		 *
		 * @return lazy {@link MonoCacheBuilderCacheWriter}
		 */
		MonoCacheBuilderCacheWriter<KEY, VALUE> onCacheMissResume(Mono<? extends VALUE> other);
	}

	/**
	 * Builder that setup {@link MonoCacheWriter}
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	@FunctionalInterface
	interface MonoCacheBuilderCacheWriter<KEY, VALUE> {

		/**
		 * Setup {@link MonoCacheWriter}
		 *
		 * @param writer {@link MonoCacheWriter} instance
		 *
		 * @return {@link Mono}
		 */
		Mono<VALUE> andWriteWith(MonoCacheWriter<? super KEY, VALUE> writer);
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
	 * @param <VALUE> type
	 */
	@FunctionalInterface
	interface MonoCacheBuilderMapMiss<VALUE> {

		/**
		 * Setup original source
		 *
		 * @param other original source
		 *
		 * @return direct {@link Mono}
		 */
		Mono<VALUE> onCacheMissResume(Mono<? extends VALUE> other);
	}

	// ==== Flux Builders ====

	/**
	 * Builder that setup original source
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	@FunctionalInterface
	interface FluxCacheBuilderCacheMiss<KEY, VALUE> {

		/**
		 * Setup original source
		 *
		 * @param other original source
		 *
		 * @return lazy {@link FluxCacheBuilderCacheWriter}
		 */
		FluxCacheBuilderCacheWriter<KEY, VALUE> onCacheMissResume(Flux<VALUE> other);
	}

	/**
	 * Builder that setup {@link FluxCacheWriter}
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	@FunctionalInterface
	interface FluxCacheBuilderCacheWriter<KEY, VALUE> {

		/**
		 * Setup {@link FluxCacheWriter}
		 *
		 * @param writer {@link FluxCacheWriter} instance
		 *
		 * @return {@link Flux}
		 */
		Flux<VALUE> andWriteWith(FluxCacheWriter<? super KEY, VALUE> writer);
	}

	/**
	 * Cache builder that adapt {@link Map} as the Cache source and skip building steps
	 * such as specified by {@link FluxCacheBuilderCacheWriter}
	 *
	 * <p> Example usage:
	 * <pre><code>
	 *    LoadingCache<String, Object> graphs = Caffeine.newBuilder()
	 *                                       .maximumSize(10_000)
	 *                                       .expireAfterWrite(5, TimeUnit.MINUTES)
	 *                                       .refreshAfterWrite(1, TimeUnit.MINUTES)
	 *                                       .build(key -> createExpensiveGraph(key));
	 *
	 *    keyStream.concatMap(key -> Cache.lookupFlux(graphs.asMap(), key)
	 *                                    .onCacheMissResume(repository.findAllByName(key))
	 * </code></pre>
	 * </p>
	 *
	 * @param <VALUE> type
	 */
	@FunctionalInterface
	interface FluxCacheBuilderMapMiss<VALUE> {

		/**
		 * Setup original source
		 *
		 * @param other original source
		 *
		 * @return direct {@link Flux}
		 */
		Flux<VALUE> onCacheMissResume(Flux<? extends VALUE> other);
	}



}
