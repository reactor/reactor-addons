package reactor.cache;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;

public class CacheHelperTest {

	@Test
	public void shouldCacheValueInMap() {
		Flux<Integer> source = Flux.just(1, 2, 3, 4);
		Map<Integer, Signal<? extends String>> cache = new HashMap<>();
		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(cache, key)
				                  .onCacheMissResume(Mono.just(key)
				                                         .transform(delay()));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("1")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("2")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("3")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("4")
		            .expectComplete()
		            .verify();

		Assert.assertEquals(4, cache.size());
	}

	@Test
	public void shouldCacheValue() {
		Flux<Integer> source = Flux.just(1, 2, 3, 4);
		Map<Integer, Signal<? extends String>> cache = new HashMap<>();

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(reader(cache), key)
				                  .onCacheMissResume(Mono.just(key)
				                                         .transform(delay()))
				                  .andWriteWith(writer(cache));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("1")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("2")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("3")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("4")
		            .expectComplete()
		            .verify();

		Assert.assertEquals(4, cache.size());
	}

	@Test
	public void shouldRestoreValueFromMapCache() {
		Flux<Integer> source = Flux.just(1, 3);

		Map<Integer, Signal<? extends String>> cache = new HashMap<>();
		cache.put(1, Signal.next("2"));

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(cache, key)
				                  .onCacheMissResume(Mono.just(key)
				                                         .transform(delay()));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap))
		            .expectSubscription()
		            .expectNext("2")
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("3")
		            .expectComplete()
		            .verify();

		Assert.assertEquals(2, cache.size());
	}

	@Test
	public void shouldRestoreValueFromCache() {
		Flux<Integer> source = Flux.just(1, 3);

		Map<Integer, Signal<? extends String>> cache = new HashMap<>();
		cache.put(1, Signal.next("2"));

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(reader(cache), key)
				                  .onCacheMissResume(Mono.just(key)
				                                         .transform(delay()))
				                  .andWriteWith(writer(cache));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap))
		            .expectSubscription()
		            .expectNext("2")
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("3")
		            .expectComplete()
		            .verify();

		Assert.assertEquals(2, cache.size());
	}

	@Test
	public void shouldCacheNullHolderInMap() {
		Flux<Integer> source = Flux.just(1, 1, 1, 1);
		Map<Integer, Signal<? extends String>> cache = new HashMap<>();

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(cache, key)
				                  .onCacheMissResume(Mono.just("")
				                                         .transform(delay())
				                                         .flatMap(v -> Mono.empty()));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap)
		                                         .materialize()
		                                         .repeat(4))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext(Signal.complete())
		            .expectNext(Signal.complete())
		            .expectNext(Signal.complete())
		            .expectNext(Signal.complete())
		            .expectComplete()
		            .verify();

		Assert.assertEquals(1, cache.size());
	}

	@Test
	public void shouldCacheNullHolder() {
		Flux<Integer> source = Flux.just(1, 1, 1, 1);
		Map<Integer, Signal<? extends String>> cache = new HashMap<>();

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(reader(cache), key)
				                  .onCacheMissResume(Mono.just("")
				                                         .transform(delay())
				                                         .flatMap(v -> Mono.empty()))
				                  .andWriteWith(writer(cache));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap)
		                                         .materialize()
		                                         .repeat(4))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext(Signal.complete())
		            .expectNext(Signal.complete())
		            .expectNext(Signal.complete())
		            .expectNext(Signal.complete())
		            .expectComplete()
		            .verify();

		Assert.assertEquals(1, cache.size());
	}

	@Test
	public void shouldCacheErrorInMap() {
		Flux<Integer> source = Flux.just(1);
		Map<Integer, Signal<? extends String>> cache = new HashMap<>();
		NullPointerException npe = new NullPointerException();

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(cache, key)
				                  .onCacheMissResume(Mono.just("")
				                                         .transform(delay())
				                                         .flatMap(v -> Mono.error(npe)));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap)
		                                         .materialize()
		                                         .repeat(4))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext(Signal.error(npe))
		            .expectNext(Signal.error(npe))
		            .expectNext(Signal.error(npe))
		            .expectNext(Signal.error(npe))
		            .expectComplete()
		            .verify();

		Assert.assertEquals(1, cache.size());
	}

	@Test
	public void shouldCacheError() {
		Flux<Integer> source = Flux.just(1);
		Map<Integer, Signal<? extends String>> cache = new HashMap<>();
		NullPointerException npe = new NullPointerException();

		Function<Integer, Mono<String>> flatMap =
				key -> CacheHelper.lookupMono(reader(cache), key)
				                  .onCacheMissResume(Mono.just("")
				                                         .transform(delay())
				                                         .flatMap(v -> Mono.error(npe)))
				                  .andWriteWith(writer(cache));

		StepVerifier.withVirtualTime(() -> source.concatMap(flatMap)
		                                         .materialize()
		                                         .repeat(4))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext(Signal.error(npe))
		            .expectNext(Signal.error(npe))
		            .expectNext(Signal.error(npe))
		            .expectNext(Signal.error(npe))
		            .expectComplete()
		            .verify();

		Assert.assertEquals(1, cache.size());
	}

	private static <Key, Value> CacheHelper.MonoCacheReader<Key, Value> reader(Map<Key, ? extends Signal<? extends Value>> cache) {
		return key -> Mono.justOrEmpty(cache.get(key));
	}

	private static <Key, Value> CacheHelper.MonoCacheWriter<Key, Value> writer(Map<Key, ? super Signal<? extends Value>> cache) {
		return (key, value) -> {
			cache.put(key, value);
			return Mono.just(value);
		};
	}

	private static Function<Mono<?>, Mono<String>> delay() {
		return in -> in.map(String::valueOf)
		               .delayElement(Duration.ofMillis(1000));
	}
}