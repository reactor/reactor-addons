package reactor.cache;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ReactorCacheTest {

	@Test
	public void simpleFluxCache() {
		Flux<Integer> source = Flux.just(4567, 6789);
		Map<String, List<Integer>> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.cache(source.delaySubscription(Duration.ofMillis(
				1000))
		                                                     .collectList())
		                                        .in(cacheStore)
		                                        .by("test"))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext(Arrays.asList(4567, 6789))
		            .expectComplete()
		            .verify();

		Assert.assertTrue(cacheStore.containsKey("test"));
		Assert.assertEquals(1, cacheStore.size());
	}

	@Test
	public void fluxCache() {
		Flux<Integer> upstream = Flux.just(4567);
		Map<String, List<String>> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .by(String::valueOf)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Flux.fromArray(key.split(
				                                        ""))
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))
		                                                                   .collectList())
		                                        .flatMapIterable(i -> i))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("4", "5", "6", "7")
		            .expectComplete()
		            .verify();

		Assert.assertTrue(cacheStore.containsKey("4567"));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertArrayEquals(Arrays.asList("4", "5", "6", "7")
		                               .toArray(),
				cacheStore.get("4567")
				          .toArray());
	}

	@Test
	public void fluxCacheWithNoExtractor() {
		Flux<Integer> upstream = Flux.just(4567);
		Map<Integer, List<String>> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Flux.fromArray(key.toString()
		                                                                                 .split(""))
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))
		                                                                   .collectList())
		                                        .flatMapIterable(i -> i))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("4", "5", "6", "7")
		            .expectComplete()
		            .verify();

		Assert.assertTrue(cacheStore.containsKey(4567));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertArrayEquals(Arrays.asList("4", "5", "6", "7")
		                               .toArray(),
				cacheStore.get(4567)
				          .toArray());
	}

	@Test
	public void monoCache() {
		Mono<Integer> upstream = Mono.just(4567);
		Map<String, String> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .by(String::valueOf)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Mono.just(key)
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("4567")
		            .expectComplete()
		            .verify();

		Assert.assertTrue(cacheStore.containsKey("4567"));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertEquals("4567", cacheStore.get("4567"));
	}

	@Test
	public void monoCacheWithNoExtractor() {
		Mono<Integer> upstream = Mono.just(4567);
		Map<Integer, String> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Mono.just(key.toString())
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1000))
		            .expectNext("4567")
		            .expectComplete()
		            .verify();

		Assert.assertTrue(cacheStore.containsKey(4567));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertEquals("4567", cacheStore.get(4567));
	}

	@Test
	public void fluxFromCache() {
		Flux<Integer> upstream = Flux.just(4567);
		Map<String, List<String>> cacheStore = new HashMap<>();
		cacheStore.put("4567", Arrays.asList("4", "5", "6", "7"));
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .by(String::valueOf)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Flux.fromArray(key.split(
				                                        ""))
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))
		                                                                   .collectList())
		                                        .flatMapIterable(i -> i))
		            .expectSubscription()
		            .expectNext("4", "5", "6", "7")
		            .expectComplete()
		            .verify(Duration.ofMillis(100));

		Assert.assertTrue(cacheStore.containsKey("4567"));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertArrayEquals(Arrays.asList("4", "5", "6", "7")
		                               .toArray(),
				cacheStore.get("4567")
				          .toArray());
	}

	@Test
	public void monoFromCache() {
		Mono<Integer> upstream = Mono.just(4567);
		Map<String, String> cacheStore = new HashMap<>();
		cacheStore.put("4567", "4567");
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .by(String::valueOf)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Mono.just(key)
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))))
		            .expectSubscription()
		            .expectNext("4567")
		            .expectComplete()
		            .verify(Duration.ofMillis(100));

		Assert.assertTrue(cacheStore.containsKey("4567"));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertEquals("4567", cacheStore.get("4567"));
	}

	@Test
	public void severalValuesFlux() {
		Flux<Integer> upstream = Flux.just(4567, 78910, 111213);
		Map<String, String> cacheStore = new HashMap<>();
		cacheStore.put("4567", "4567");
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .by(String::valueOf)
		                                        .in(cacheStore)
		                                        .computeIfEmpty(key -> Mono.just(key)
		                                                                   .delaySubscription(
				                                                                   Duration.ofMillis(
						                                                                   1000))))
		            .expectSubscription()
		            .expectNext("4567")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("78910")
		            .thenAwait(Duration.ofMillis(1000))
		            .expectNext("111213")
		            .expectComplete()
		            .verify();

		Assert.assertEquals(3, cacheStore.size());
		Assert.assertEquals("4567", cacheStore.get("4567"));
		Assert.assertEquals("78910", cacheStore.get("78910"));
		Assert.assertEquals("111213", cacheStore.get("111213"));
	}
}