package reactor.cache;

import java.time.Duration;
import java.util.ArrayList;
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
	public void fluxCache() {
		Flux<Integer> upstream = Flux.just(4567);
		Map<String, List<String>> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .key(String::valueOf)
		                                        .in(cacheStore)
		                                        .extract(Flux::fromIterable)
		                                        .or(key -> Flux.fromArray(key.split(""))
		                                                       .delaySubscription(Duration.ofMillis(
				                                                       1000)))
		                                        .merge(ArrayList::new, (list, next) -> {
			                                        list.add(next);
			                                        return list;
		                                        })
		                                        .map(Object::toString))
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
	public void monoCache() {
		Flux<Integer> upstream = Flux.just(4567);
		Map<String, String> cacheStore = new HashMap<>();
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .key(String::valueOf)
		                                        .in(cacheStore)
		                                        .extract(Mono::just)
		                                        .orMono(key -> Mono.just(key)
		                                                           .delaySubscription(
				                                                           Duration.ofMillis(
						                                                           1000)))
		                                        .map(Object::toString))
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
	public void fluxFromCache() {
		Flux<Integer> upstream = Flux.just(4567);
		Map<String, List<String>> cacheStore = new HashMap<>();
		cacheStore.put("4567", Arrays.asList("4", "5", "6", "7"));
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .key(String::valueOf)
		                                        .in(cacheStore)
		                                        .extract(Flux::fromIterable)
		                                        .or(key -> Flux.fromArray(key.split(""))
		                                                       .delaySubscription(Duration.ofMillis(
				                                                       1000)))
		                                        .merge(ArrayList::new, (list, next) -> {
			                                        list.add(next);
			                                        return list;
		                                        })
		                                        .map(Object::toString))
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
		Flux<Integer> upstream = Flux.just(4567);
		Map<String, String> cacheStore = new HashMap<>();
		cacheStore.put("4567", "4567");
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .key(String::valueOf)
		                                        .in(cacheStore)
		                                        .extract(Mono::just)
		                                        .orMono(key -> Mono.just(key)
		                                                           .delaySubscription(
				                                                           Duration.ofMillis(
						                                                           1000)))
		                                        .map(Object::toString))
		            .expectSubscription()
		            .expectNext("4567")
		            .expectComplete()
		            .verify(Duration.ofMillis(100));

		Assert.assertTrue(cacheStore.containsKey("4567"));
		Assert.assertEquals(1, cacheStore.size());
		Assert.assertEquals("4567", cacheStore.get("4567"));
	}

	@Test
	public void severalValuesMono() {
		Flux<Integer> upstream = Flux.just(4567, 78910, 111213);
		Map<String, String> cacheStore = new HashMap<>();
		cacheStore.put("4567", "4567");
		StepVerifier.withVirtualTime(() -> Cache.from(upstream)
		                                        .key(String::valueOf)
		                                        .in(cacheStore)
		                                        .extract(Mono::just)
		                                        .orMono(key -> Mono.just(key)
		                                                           .delaySubscription(
				                                                           Duration.ofMillis(
						                                                           1000)))
		                                        .map(Object::toString))
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
