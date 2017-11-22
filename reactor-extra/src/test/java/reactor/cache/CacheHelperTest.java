package reactor.cache;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import static org.assertj.core.api.Assertions.assertThat;

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

	@Test
	public void monoCacheFromMapIsLazy() {
		Map<String, Signal<? extends Integer>> data = new HashMap<>();
		PublisherProbe<Integer> probe = PublisherProbe.of(Mono.defer(() -> {
			if (data.isEmpty()) return Mono.just(1);
			return Mono.error(new IllegalStateException("shouldn't go there"));
		}));

		Mono<Integer> test = CacheHelper.lookupMono(data, "foo")
				.onCacheMissResume(probe.mono());

		assertThat(test.block()).isEqualTo(1);

		probe.assertWasSubscribed();
		probe.assertWasRequested();
		assertThat(data).containsEntry("foo", Signal.next(1));

		StepVerifier.create(test)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void monoCacheFromWriterIsLazy() {
		Map<String, Signal<? extends Integer>> data = new HashMap<>();
		PublisherProbe<Integer> probe = PublisherProbe.of(Mono.defer(() -> {
			if (data.isEmpty()) return Mono.just(1);
			return Mono.error(new IllegalStateException("shouldn't go there"));
		}));

		Mono<Integer> test = CacheHelper.lookupMono(reader(data), "foo")
				.onCacheMissResume(probe.mono())
				.andWriteWith(writer(data));

		assertThat(test.block()).isEqualTo(1);

		probe.assertWasSubscribed();
		probe.assertWasRequested();
		assertThat(data).containsEntry("foo", Signal.next(1));

		StepVerifier.create(test)
		            .expectNext(1)
		            .verifyComplete();
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

	@Test
	public void fluxCacheFromMapMiss() {
		Map<String, List<?>> data = new HashMap<>();

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(data, "foo")
		.onCacheMissResume(Flux.just(1, 2, 3));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(data).containsKey("foo");
		final List<?> list = data.get("foo");
		assertThat(list.remove(3))
				.isInstanceOfSatisfying(Signal.class, Signal::isOnComplete);
		assertThat(list).hasSize(3)
		                .allMatch(p -> p instanceof Signal && ((Signal) p).isOnNext());
	}

	@Test
	public void fluxCacheFromMapHitComplete() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.just(1, 2, 3));
		Map<String, List<?>> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.complete()));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(data, "foo")
		.onCacheMissResume(probe.flux());

		StepVerifier.create(test)
		            .expectNext(4, 5, 6)
		            .verifyComplete();

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromMapHitError() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.just(1, 2, 3));
		Map<String, List<?>> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.error(new IllegalStateException("boom"))));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(data, "foo")
		.onCacheMissResume(probe.flux());

		StepVerifier.create(test)
		            .expectNext(4, 5, 6)
		            .verifyErrorMessage("boom");

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromMapHitEmpty() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.empty());
		Map<String, List<?>> data = new HashMap<>();
		data.put("foo", Collections.singletonList(Signal.complete()));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(data, "foo")
		.onCacheMissResume(probe.flux());

		StepVerifier.create(test)
		            .verifyComplete();

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromMapLazy() {
		Map<String, List<?>> data = new HashMap<>();
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.defer(() -> {
			if (data.isEmpty()) return Flux.just(1, 2);
			return Flux.error(new IllegalStateException("shouldn't go there"));
		}));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(data, "foo")
		.onCacheMissResume(probe.flux());

		assertThat(test.collectList().block()).containsExactly(1, 2);

		probe.assertWasSubscribed();
		probe.assertWasRequested();
		assertThat(data).containsKey("foo");
		assertThat(data.get("foo")).hasSize(3);

		StepVerifier.create(test)
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void fluxCacheFromWriterMiss() {
		Map<String, List<?>> data = new HashMap<>();

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(readerFlux(data), "foo")
		                                .onCacheMissResume(Flux.just(1, 2, 3))
		                                .andWriteWith(writerFlux(data));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(data).containsKey("foo");
		final List<?> list = data.get("foo");
		assertThat(list.remove(3))
				.isInstanceOfSatisfying(Signal.class, Signal::isOnComplete);
		assertThat(list).hasSize(3)
		                .allMatch(p -> p instanceof Signal && ((Signal) p).isOnNext());
	}

	@Test
	public void fluxCacheFromWriterHitComplete() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.just(1, 2, 3));
		Map<String, List<?>> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.complete()));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(readerFlux(data), "foo")
				.onCacheMissResume(probe.flux())
				.andWriteWith(writerFlux(data));

		StepVerifier.create(test)
		            .expectNext(4, 5, 6)
		            .verifyComplete();

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromWriterHitError() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.just(1, 2, 3));
		Map<String, List<?>> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.error(new IllegalStateException("boom"))));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(readerFlux(data), "foo")
				.onCacheMissResume(probe.flux())
				.andWriteWith(writerFlux(data));

		StepVerifier.create(test)
		            .expectNext(4, 5, 6)
		            .verifyErrorMessage("boom");

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromWriterHitEmpty() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.empty());
		Map<String, List<?>> data = new HashMap<>();
		data.put("foo", Collections.singletonList(Signal.complete()));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(readerFlux(data), "foo")
				.onCacheMissResume(probe.flux())
				.andWriteWith(writerFlux(data));

		StepVerifier.create(test)
		            .verifyComplete();

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromWriterLazy() {
		Map<String, List<?>> data = new HashMap<>();
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.defer(() -> {
			if (data.isEmpty()) return Flux.just(1, 2);
			return Flux.error(new IllegalStateException("shouldn't go there"));
		}));

		Flux<Integer> test = CacheHelper.<String, Integer>lookupFlux(readerFlux(data), "foo")
				.onCacheMissResume(probe.flux())
				.andWriteWith(writerFlux(data));

		assertThat(test.collectList().block()).containsExactly(1, 2);

		probe.assertWasSubscribed();
		probe.assertWasRequested();
		assertThat(data).containsKey("foo");
		assertThat(data.get("foo")).hasSize(3);

		StepVerifier.create(test)
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	private <K, T> CacheHelper.FluxCacheReader<K, T> readerFlux(Map<K, List<?>> data) {
		return k -> Mono.justOrEmpty((List<Signal<T>>) data.get(k));
	}

	private <K, T> CacheHelper.FluxCacheWriter<K, T> writerFlux(Map<K, List<?>> data) {
		return ((k, signals) -> {
			data.put(k, signals);
			return Mono.just(signals);
		});
	}

}