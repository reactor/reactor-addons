package reactor.cache;

import java.util.Map;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
final class FluxCacheBuilder
		implements Cache.FluxCache.KeyExtractorRequired, Cache.FluxCache.CacheRequired,
		           Cache.FluxCache.DownstreamRequired {

	private final Flux     upstream;
	private       Map      cacheMap;
	private       Function keyExtractor;

	FluxCacheBuilder(Flux upstream) {
		this.upstream = upstream;
		this.keyExtractor = Function.identity();
	}

	@Override
	public Cache.FluxCache.CacheRequired by(Function keyExtractor) {
		this.keyExtractor = keyExtractor;
		return this;
	}

	@Override
	public Cache.FluxCache.DownstreamRequired in(Map cacheMap) {
		this.cacheMap = cacheMap;
		return this;
	}

	@Override
	public Flux computeIfEmpty(Function downstreamFunction) {

		return upstream.flatMap(in -> {
			Object key = keyExtractor.apply(in);
			Object cachedOut = cacheMap.get(key);

			if (cachedOut != null) {
				return Mono.just(cachedOut);
			}
			else {
				return ((Mono) downstreamFunction.apply(key)).doOnNext(n -> cacheMap.put(
						key,
						n));
			}
		});
	}
}
