package reactor.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class CacheFluxTest {


	private Function<String, Mono<List<Signal<Integer>>>> reader(Map<String, List> data) {
		return k -> Mono.justOrEmpty(data.get(k));
	}

	private BiFunction<String, List<Signal<Integer>>, Mono<Void>> writer(Map<String, List> data) {
		return ((k, signals) -> Mono.fromRunnable(() -> data.put(k, signals)));
	}

	@Test
	public void genericEntryPointJavadocExample() {
		String key = "burger";
		AtomicInteger sourceSubscribed = new AtomicInteger();
		Flux<Integer> source = Flux.range(1, 10).doOnSubscribe(it -> sourceSubscribed.incrementAndGet());
		AtomicReference<Context> storeRef = new AtomicReference<>(Context.empty());

		Flux<Integer> cachedFlux = CacheFlux
				.lookup(k -> Mono.justOrEmpty(storeRef.get().getOrEmpty(k))
				                 .cast(Integer.class)
				                 .flatMap(max -> Flux.range(1, max)
				                                     .materialize()
				                                     .collectList()),
						key)
				.onCacheMissResume(source)
				.andWriteWith((k, sigs) -> Flux.fromIterable(sigs)
				                               .dematerialize()
				                               .last()
				                               .doOnNext(max -> storeRef.updateAndGet(ctx -> ctx.put(k, max)))
				                               .then());

		assertThat(sourceSubscribed).as("before use").hasValue(0);

		StepVerifier.create(cachedFlux)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .expectComplete()
		            .verify();

		assertThat(sourceSubscribed).as("cache miss").hasValue(1);

		assertThat(storeRef.get())
				.matches(ctx -> ctx.hasKey(key), "hasKey")
				.satisfies(ctx -> assertThat(ctx.getOrDefault(key, -1)).isEqualTo(10));

		StepVerifier.create(cachedFlux)
		            .expectNextCount(10)
		            .expectComplete()
		            .verify();

		assertThat(sourceSubscribed).as("cache hit").hasValue(1);
	}


	@Test
	public void fluxCacheFromMapMiss() {
		Map<String, List> data = new HashMap<>();

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
		                              .onCacheMissResume(Flux.just(1, 2, 3));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(data).containsKey("foo");
		final List<Object> list = data.get("foo");
		assertThat(list.remove(3))
				.isInstanceOfSatisfying(Signal.class, Signal::isOnComplete);
		assertThat(list).hasSize(3)
		                .allMatch(p -> p instanceof Signal && ((Signal) p).isOnNext());
	}

	@Test
	public void fluxCacheFromMapHitComplete() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.just(1, 2, 3));
		Map<String, List> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.complete()));

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
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
		Map<String, List> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.error(new IllegalStateException("boom"))));

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
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
		Map<String, List> data = new HashMap<>();
		data.put("foo", Collections.singletonList(Signal.complete()));

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
		                              .onCacheMissResume(probe.flux());

		StepVerifier.create(test)
		            .verifyComplete();

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromMapLazy() {
		Map<String, List> data = new HashMap<>();
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.defer(() -> {
			if (data.isEmpty()) return Flux.just(1, 2);
			return Flux.error(new IllegalStateException("shouldn't go there"));
		}));

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
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
		Map<String, List> data = new HashMap<>();

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(Flux.just(1, 2, 3))
		                              .andWriteWith(writer(data));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(data).containsKey("foo");
		final List<Object> list = data.get("foo");
		assertThat(list.remove(3))
				.isInstanceOfSatisfying(Signal.class, Signal::isOnComplete);
		assertThat(list).hasSize(3)
		                .allMatch(p -> p instanceof Signal && ((Signal) p).isOnNext());
	}

	@Test
	public void fluxCacheFromWriterHitComplete() {
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.just(1, 2, 3));
		Map<String, List> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.complete()));

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(probe.flux())
		                              .andWriteWith(writer(data));

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
		Map<String, List> data = new HashMap<>();
		data.put("foo", Arrays.asList(Signal.next(4), Signal.next(5), Signal.next(6), Signal.error(new IllegalStateException("boom"))));

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(probe.flux())
		                              .andWriteWith(writer(data));

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
		Map<String, List> data = new HashMap<>();
		data.put("foo", Collections.singletonList(Signal.complete()));

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(probe.flux())
		                              .andWriteWith(writer(data));

		StepVerifier.create(test)
		            .verifyComplete();

		probe.assertWasNotSubscribed();
		probe.assertWasNotCancelled();
		probe.assertWasNotRequested();
	}

	@Test
	public void fluxCacheFromWriterLazy() {
		Map<String, List> data = new HashMap<>();
		PublisherProbe<Integer> probe = PublisherProbe.of(Flux.defer(() -> {
			if (data.isEmpty()) return Flux.just(1, 2);
			return Flux.error(new IllegalStateException("shouldn't go there"));
		}));

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(probe.flux())
		                              .andWriteWith(writer(data));

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
	public void writerShouldLazilyResolveSource() {
		AtomicLong aLong = new AtomicLong();
		Map<String, List> data = new HashMap<>();
		Supplier<Flux<Integer>> sourceSupplier = () -> Flux.just(aLong.intValue());

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(sourceSupplier)
		                              .andWriteWith(writer(data));

		aLong.set(3L);

		StepVerifier.create(test)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void writerNoSupplierShouldCaptureSource() {
		AtomicLong aLong = new AtomicLong();
		Map<String, List> data = new HashMap<>();

		Flux<Integer> test = CacheFlux.lookup(reader(data), "foo")
		                              .onCacheMissResume(Flux.just(aLong.intValue()))
		                              .andWriteWith(writer(data));

		aLong.set(3L);

		StepVerifier.create(test)
		            .expectNext(0)
		            .verifyComplete();
	}

	@Test
	public void mapShouldLazilyResolveSource() {
		AtomicLong aLong = new AtomicLong();
		Map<String, List> data = new HashMap<>();
		Supplier<Flux<Integer>> sourceSupplier = () -> Flux.just(aLong.intValue());

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
		                              .onCacheMissResume(sourceSupplier);

		aLong.set(3L);

		StepVerifier.create(test)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void mapNoSupplierShouldCaptureSource() {
		AtomicLong aLong = new AtomicLong();
		Map<String, List> data = new HashMap<>();

		Flux<Integer> test = CacheFlux.lookup(data, "foo", Integer.class)
		                              .onCacheMissResume(Flux.just(aLong.intValue()));

		aLong.set(3L);

		StepVerifier.create(test)
		            .expectNext(0)
		            .verifyComplete();
	}

	@Test
	public void mapWithSpecificType() {
		Map<Object, List> cacheMap = new HashMap<>();
		Object key1 = new Object();

		Flux<Integer> test = CacheFlux.lookup(cacheMap, key1, Integer.class)
		                              .onCacheMissResume(Flux.range(1, 5));

		StepVerifier.create(test)
		            .expectNext(1, 2,3, 4, 5)
		            .verifyComplete();

		assertThat(cacheMap).hasSize(1);
	}

	@Test
	public void mapWithRelaxedTypes() {
		Map<Object, Object> cacheMap = new HashMap<>();
		Object key1 = new Object();

		Flux<Integer> test = CacheFlux.lookup(cacheMap, key1, Integer.class)
		                              .onCacheMissResume(Flux.range(1, 5));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();

		assertThat(cacheMap).hasSize(1);
	}

}
