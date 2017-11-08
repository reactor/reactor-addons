package reactor.cache;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class Cache {

	public static <T> KeyExtractorRequired<T> from(Publisher<T> upstream) {
		return new Builder(upstream);
	}

	private static final class Builder
			implements KeyExtractorRequired, CacheRequired, ValueExtractorRequired,
			           DownstreamRequired, MergeRequired {

		private final Publisher upstream;
		private       Map       cacheMap;
		private       Function  downstreamFunction;
		private       Function  keyExtractor;
		private       Function  valueExtractor;

		private Builder(Publisher upstream) {
			this.upstream = upstream;
		}

		@Override
		public CacheRequired key(Function keyExtractor) {
			this.keyExtractor = keyExtractor;
			return this;
		}

		@Override
		public ValueExtractorRequired in(Map cacheMap) {
			this.cacheMap = cacheMap;
			return this;
		}

		@Override
		public DownstreamRequired extract(Function valueExtractor) {
			this.valueExtractor = valueExtractor;
			return this;
		}

		@Override
		public MergeRequired or(Function downstreamFunction) {
			this.downstreamFunction = downstreamFunction;
			return this;
		}

		@Override
		public Flux orMono(Function downstreamFunction) {
			this.downstreamFunction = downstreamFunction;
			return apply(() -> null, (a, n) -> n);
		}

		@Override
		public Flux merge(Supplier supplier, BiFunction merger) {
			return apply(supplier, merger);
		}

		private Flux apply(Supplier supplier, BiFunction merger) {
			return Flux.from(upstream)
			           .flatMap(in -> {
				           Object key = keyExtractor.apply(in);

				           Object cachedOut = cacheMap.get(key);

				           if (cachedOut != null) {
					           return valueExtractor.apply(cachedOut);
				           }
				           else {
					           return Flux.from((Publisher) downstreamFunction.apply(key))
					                      .doOnNext(n -> {
						                      Object state = cacheMap.computeIfAbsent(key,
								                      k -> supplier.get());

						                      cacheMap.put(key, merger.apply(state, n));
					                      });
				           }
			           });
		}
	}

	public interface KeyExtractorRequired<In> {

		<Key> CacheRequired<Key> key(Function<? super In, ? extends Key> extractor);
	}

	public interface CacheRequired<Key> {

		<Value> ValueExtractorRequired<Key, Value> in(Map<? super Key, ? super Value>
				cacheMap);
	}

	public interface ValueExtractorRequired<Key, Value> {

		<Out> DownstreamRequired<Key, Out> extract(Function<? super Value, Publisher<? extends Out>> extractor);
	}

	public interface DownstreamRequired<Key, Out> {

		MergeRequired<Out> or(Function<? super Key, Publisher<? extends Out>> downstreamFunction);

		Flux<Out> orMono(Function<? super Key, Mono<? extends Out>> downstreamFunction);
	}

	public interface MergeRequired<Out> {

		<Collector> Flux<Out> merge(Supplier<? super Collector> supplier,
				BiFunction<Collector, Out, Collector> merger);
	}
}
