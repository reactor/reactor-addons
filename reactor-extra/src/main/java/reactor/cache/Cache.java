package reactor.cache;

import java.util.Map;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class Cache {

	public static <T> FluxCache.KeyExtractorRequired<T> from(Flux<T> upstream) {
		return new FluxCacheBuilder(upstream);
	}

	public static <T> MonoCache.KeyExtractorRequired<T> from(Mono<T> upstream) {
		return new MonoCacheBuilder(upstream);
	}

	public interface MonoCache {

		interface KeyExtractorRequired<In> extends CacheRequired<In> {

			<Key> CacheRequired<Key> by(Function<? super In, ? extends Key> extractor);
		}

		interface CacheRequired<Key> {

			<Out> DownstreamRequired<Key, Out> in(Map<? super Key, ? super Out> cacheMap);
		}

		interface DownstreamRequired<Key, Out> {

			Mono<Out> computeIfEmpty(Function<? super Key, Mono<? extends Out>> downstreamFunction);
		}
	}

	public interface FluxCache {

		interface KeyExtractorRequired<In> extends CacheRequired<In> {

			<Key> CacheRequired<Key> by(Function<? super In, ? extends Key> extractor);
		}

		interface CacheRequired<Key> {

			<Out> DownstreamRequired<Key, Out> in(Map<? super Key, ? super Out> cacheMap);
		}

		interface DownstreamRequired<Key, Out> {

			Flux<Out> computeIfEmpty(Function<? super Key, Mono<? extends Out>> downstreamFunction);
		}
	}
}
