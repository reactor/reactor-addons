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

import javax.swing.SingleSelectionModel;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

/**
 * Opinionated caching helper that defines how to store and restore a {@link Mono} in an
 * arbitrary cache abstraction. A generic writer/reader entry point is provided, but cache
 * vendors that have a Map wrapper support can also be directly used.
 * <p>
 * Generic entry points example:
 * <pre><code>
 *     AtomicReference&lt;Context> storeRef = new AtomicReference<>(Context.empty());
 *
 *     Mono&lt;Integer> cachedMono = CacheMono
 *     		.lookup(k -> Mono.justOrEmpty(storeRef.get().&lt;Integer>getOrEmpty(k))
 *     		                 .map(Signal::next),
 *     		        key)
 *     		.onCacheMissResume(Mono.just(123))
 *     		.andWriteWith((k, sig) -> Mono.fromRunnable(() ->
 *     	            storeRef.updateAndGet(ctx -> ctx.put(k, sig.get()))));
 * </code></pre>
 * <p>
 * Map entry points example:
 * <pre><code>
 *    String key = "myId";
 *    LoadingCache&lt;String, Object> graphs = Caffeine
 *        .newBuilder()
 *        .maximumSize(10_000)
 *        .expireAfterWrite(5, TimeUnit.MINUTES)
 *        .refreshAfterWrite(1, TimeUnit.MINUTES)
 *        .build(key -> createExpensiveGraph(key));
 *
 *    Mono&lt;Integer> cachedMyId = CacheMono
 *        .lookup(graphs.asMap(), key)
 *        .onCacheMissResume(repository.findOneById(key));
 * </code></pre>
 * </p>
 *
 * @author Oleh Dokuka
 * @author Simon Baslé
 */
public class CacheMono {
	
	private CacheMono() {
	}

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
	 * <p>
	 * For maps that are too generic (eg. {@code Map<String, Object>}), a variant is provided
	 * that allows to refine the resulting {@link Mono} value type by explicitly providing
	 * a {@link Class}.
	 *
	 * @param cacheMap {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link MonoCacheBuilderMapMiss builder step} to use to set up the source
	 * @see #lookup(Map, Object, Class)
	 */
	public static <KEY, VALUE> MonoCacheBuilderMapMiss<VALUE> lookup(Map<KEY, ? super Signal<? extends VALUE>> cacheMap, KEY key) {
		return otherSupplier -> Mono.defer(() -> {
					Object fromCache = cacheMap.get(key);
					if (fromCache == null) {
						return otherSupplier.get()
								.materialize()
								.doOnNext(value -> cacheMap.put(key, value))
								.dematerialize();
					}
					if (fromCache instanceof Signal) {
						return Mono.just(fromCache).dematerialize();
					}
					throw new IllegalArgumentException("Content of cache for key " + key + " must be a Signal");
				}
		);
	}

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
	 * <p>
	 * This variant is for maps that are too generic (eg. {@code Map<String, Object>}),
	 * helping the compiler refine the generic typ of the resulting {@link Mono} by
	 * explicitly providing a {@link Class}. Value stored in the map for the key is
	 * expected to be a {@link Signal} of a value of that {@link Class}.
	 *
	 * @param cacheMap {@link Map} wrapper of a cache
	 * @param key mapped key
	 * @param valueClass the generic {@link Class} of the resulting {@link Mono}
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link MonoCacheBuilderMapMiss builder step} to use to set up the source
	 * @see #lookup(Map, Object, Class)
	 */
	public static <KEY, VALUE> MonoCacheBuilderMapMiss<VALUE> lookup(Map<KEY, ? super Signal<? extends VALUE>> cacheMap, KEY key, Class<VALUE> valueClass) {
		return lookup(cacheMap, key);
	}

	/**
	 * Restore a {@link Mono Mono&lt;VALUE&gt;} from the {@link Function reader Function}
	 * (see below) given a provided key.
	 * If no value is in the cache, it will be calculated from the original source
	 * which is set up in the next step. Note that if the source completes empty, this
	 * result will be cached and all subsequent requests with the same key will return
	 * {@link Mono#empty()}. The behaviour is similar for erroring sources, except cache
	 * hits would then return {@link Mono#error(Throwable)}.
	 * <p>
	 * Note that the wrapped {@link Mono} is lazy, meaning that subscribing twice in a row
	 * to the returned {@link Mono} on an empty cache will trigger a cache miss then a
	 * cache hit.
	 * <p>
	 * The cache {@link Function reader Function} takes a key and returns a {@link Mono}
	 * of a {@link Signal} representing the value (or {@link Signal#complete()} if the
	 * cached Mono was empty). See {@link CacheMono} for an example.

	 *
	 * @param reader a {@link Function} that looks up {@link Signal} from a cache, returning
	 * them as a {@link Mono Mono&lt;Signal&gt;}
	 * @param key mapped key
	 * @param <KEY> Key Type
	 * @param <VALUE> Value Type
	 *
	 * @return The next {@link MonoCacheBuilderCacheMiss builder step} to use to set up the source
	 */
	public static <KEY, VALUE> MonoCacheBuilderCacheMiss<KEY, VALUE> lookup(
			Function<KEY, Mono<Signal<? extends VALUE>>> reader, KEY key) {
		return otherSupplier -> writer -> Mono.defer(() ->
				reader.apply(key)
				  .switchIfEmpty(Mono.defer(() -> otherSupplier.get())
				                              .materialize()
				                              .flatMap(signal -> writer.apply(key, signal)
				                                                       .then(Mono.just(signal))
				                              )
				  )
				  .dematerialize());
	}

	// ==== Mono Builders ====

	/**
	 * Setup original source to fallback to in case of cache miss.
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	public interface MonoCacheBuilderCacheMiss<KEY, VALUE> {

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
	 * Set up the {@link BiFunction cache writer BiFunction} to use to store the source
	 * data into the cache in case of cache miss.
	 * <p>
	 * This BiFunction takes the target key and a {@link Signal} representing the value to
	 * store, and returns a {@link Mono Mono&lt;Void&gt;} that represents the completion
	 * of the cache write operation.
	 *
	 * @param <KEY> Key type
	 * @param <VALUE> Value type
	 */
	public interface MonoCacheBuilderCacheWriter<KEY, VALUE> {

		/**
		 * Set up the {@link BiFunction cache writer BiFunction} to use to store the source
		 * data into the cache in case of cache miss. This BiFunction takes the target key
		 * and a {@link Signal} representing the value to store, and returns a {@link Mono Mono&lt;Void&gt;}
		 * that represents the completion of the cache write operation.
		 *
		 * @param writer Cache writer {@link BiFunction}
		 *
		 * @return A wrapped {@link Mono} that transparently looks up data from a cache
		 * and store data into the cache.
		 */
		Mono<VALUE> andWriteWith(BiFunction<KEY, Signal<? extends VALUE>, Mono<Void>> writer);
	}

	/**
	 * Setup original source to fallback to in case of cache miss and return a wrapped
	 * {@link Mono} that transparently looks up data from a {@link Map} representation of
	 * a cache and store data into the cache in case of cache miss.
	 *
	 * @param <VALUE> type
	 */
	public interface MonoCacheBuilderMapMiss<VALUE> {

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
