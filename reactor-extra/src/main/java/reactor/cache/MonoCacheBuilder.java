package reactor.cache;

import java.util.Map;
import java.util.function.Function;

import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
final class MonoCacheBuilder
		implements Cache.MonoCache.KeyExtractorRequired, Cache.MonoCache.CacheRequired,
		           Cache.MonoCache.DownstreamRequired {

	private final Mono     upstream;
	private       Map      cacheMap;
	private       Function keyExtractor;

	MonoCacheBuilder(Mono upstream) {
		this.upstream = upstream;
		this.keyExtractor = Function.identity();
	}

	@Override
	public Cache.MonoCache.CacheRequired by(Function keyExtractor) {
		this.keyExtractor = keyExtractor;
		return this;
	}

	@Override
	public Cache.MonoCache.DownstreamRequired in(Map cacheMap) {
		this.cacheMap = cacheMap;
		return this;
	}

	@Override
	public Mono computeIfEmpty(Function downstreamFunction) {
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