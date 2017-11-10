package reactor.cache;

import java.util.Map;

import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
final class SimpleCacheBuilder implements Cache.CacheRequired, Cache.KeyRequired {

	private final Mono mono;
	private       Map  cacheMap;

	SimpleCacheBuilder(Mono mono) {

		this.mono = mono;
	}

	@Override
	public Mono by(Object key) {
		Object value = cacheMap.get(key);
		return Mono.justOrEmpty(value)
		           .switchIfEmpty(mono)
		           .doOnNext(v -> cacheMap.put(key, v));
	}

	@Override
	public Cache.KeyRequired in(Map cacheMap) {
		this.cacheMap = cacheMap;
		return this;
	}
}