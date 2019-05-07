/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.extra.processor;

import java.awt.event.KeyEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * @author Stephane Maldini
 */
public class TopicProcessorTest {

	private static void println(final Object... fragments) {
		final Thread currentThread = Thread.currentThread();
		synchronized (System.out) {
			System.out.print(String.format("[%s] ", currentThread.getName()));
			for (final Object fragment : fragments) {
				System.out.print(fragment);
			}
			System.out.println();
		}
	}

	@Test
	@Ignore
	public void chainedTopicProcessor() throws Exception {
		ExecutorService es = Executors.newFixedThreadPool(2);
		try {
			TopicProcessor<String> bc = TopicProcessor.<String>builder().executor(es)
			                                                            .bufferSize(16)
			                                                            .build();

			int elems = 100;
			CountDownLatch latch = new CountDownLatch(elems);

			bc.subscribe(sub("spec1", latch));
			Flux.range(0, elems)
			    .map(s -> "hello " + s)
			    .subscribe(bc);

			assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		}
		finally {
			es.shutdown();
		}
	}

	@Test
	public void createDefault() {
		TopicProcessor<Integer> processor = TopicProcessor.create();
		assertProcessor(processor, false, null, null, null, null, null, null);
	}

	@Test
	public void createDefaultExecutorOverrideAll() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, name, bufferSize, waitStrategy, autoCancel, null, null);
	}

	@Test
	public void createOverrideAll() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		ExecutorService requestTaskExecutor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .requestTaskExecutor(requestTaskExecutor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, null, bufferSize, waitStrategy, autoCancel, executor, requestTaskExecutor);
	}

	@Test
	public void createOverrideAutoCancel() {
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, null, null, null, autoCancel, null, null);
	}

	@Test
	public void createOverrideExecutor() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .build();
		assertProcessor(processor, false, null, null, null, null, executor, null);
	}

	@Test
	public void createOverrideExecutorAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, null, null, null, autoCancel, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSize() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .build();
		assertProcessor(processor, false, null, bufferSize, null, null, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSizeAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, null, bufferSize, null, autoCancel, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSizeWaitStrategy() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .build();
		assertProcessor(processor, false, null, bufferSize, waitStrategy, null, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSizeWaitStrategyAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, null, bufferSize, waitStrategy, autoCancel, executor, null);
	}

	@Test
	public void createOverrideName() {
		String name = "nameOverride";
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().name(name)
		                                                                     .build();
		assertProcessor(processor, false, name, null, null, null, null, null);
	}

	@Test
	public void createOverrideNameBufferSize() {
		String name = "nameOverride";
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .build();
		assertProcessor(processor, false, name, bufferSize, null, null, null, null);
	}

	@Test
	public void createOverrideNameBufferSizeAutoCancel() {
		String name = "nameOverride";
		int bufferSize = 1024;
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, false, name, bufferSize, null, autoCancel, null, null);
	}

	@Test
	public void createOverrideNameBufferSizeWaitStrategy() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .build();
		assertProcessor(processor, false, name, bufferSize, waitStrategy, null, null, null);
	}

	@Test
	public void createSmokeTest() {
		//this build sequence has been reported as throwing an exception
		// with JDK9 (see https://github.com/reactor/reactor-core/issues/881)
		TopicProcessor.builder()
		              .share(true)
		              .build();
	}

	@Test
	public void customRequestTaskThreadRejectsNull() {
		ExecutorService customTaskExecutor = null;

		Assertions.assertThatExceptionOfType(NullPointerException.class)
		          .isThrownBy(() -> new TopicProcessor<>(Thread::new,
				          Executors.newCachedThreadPool(),
				          customTaskExecutor,
				          8,
				          WaitStrategy.liteBlocking(),
				          true,
				          true,
				          Object::new));
	}

	@Test
	public void drainTest() throws Exception {
		final TopicProcessor<Integer> sink = TopicProcessor.<Integer>builder().name("topic")
		                                                                      .build();
		sink.onNext(1);
		sink.onNext(2);
		sink.onNext(3);

		StepVerifier.create(sink.forceShutdown())
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeBufferSize() {
		TopicProcessor.builder()
		              .name("test")
		              .bufferSize(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNonPowerOfTwo() {
		TopicProcessor.builder()
		              .name("test")
		              .bufferSize(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNullBufferSize() {
		TopicProcessor.builder()
		              .name("test")
		              .bufferSize(0);
	}

	@Test
	public void indexBugTest() throws InterruptedException {
		int numOfItems = 20;

		//this line causes an java.lang.ArrayIndexOutOfBoundsException unless there is a break point in ZipAction
		// .createSubscriber()
		TopicProcessor<String> ring = TopicProcessor.<String>builder().name("test")
		                                                              .bufferSize(1024)
		                                                              .build();

//        EmitterProcessor<String> ring = EmitterProcessor.create();

		Flux<String> stream2 = ring.zipWith(Mono.fromCallable(System::currentTimeMillis)
		                                        .repeat(), (t1, t2) -> String.format("%s : %s", t1, t2));

		Mono<List<String>> p = stream2.doOnNext(System.out::println)
		                              .buffer(numOfItems)
		                              .next();

		for (int curr = 0; curr < numOfItems; curr++) {
			if (curr % 5 == 0 && curr % 3 == 0) {
				ring.onNext("FizBuz" + curr);
			}
			else if (curr % 3 == 0) {
				ring.onNext("Fiz" + curr);
			}
			else if (curr % 5 == 0) {
				ring.onNext("Buz" + curr);
			}
			else {
				ring.onNext(String.valueOf(curr));
			}
		}

		Assert.assertTrue("Has not returned list", p.block(Duration.ofSeconds(5)) != null);

	}

	@Test
	public void konamiCode() throws InterruptedException {
		final TopicProcessor<Integer> keyboardStream = TopicProcessor.create();

		Mono<List<Boolean>> konamis = keyboardStream.skipWhile(key -> KeyEvent.VK_UP != key)
		                                            .buffer(10, 1)
		                                            .map(keys -> keys.size() == 10 && keys.get(0) == KeyEvent.VK_UP && keys.get(
				                                            1) == KeyEvent.VK_UP && keys.get(2) == KeyEvent.VK_DOWN && keys.get(
				                                            3) == KeyEvent.VK_DOWN && keys.get(4) == KeyEvent.VK_LEFT && keys.get(
				                                            5) == KeyEvent.VK_RIGHT && keys.get(6) == KeyEvent.VK_LEFT && keys.get(
				                                            7) == KeyEvent.VK_RIGHT && keys.get(8) == KeyEvent.VK_B && keys.get(
				                                            9) == KeyEvent.VK_A)
		                                            .collectList();

		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_DOWN);
		keyboardStream.onNext(KeyEvent.VK_DOWN);
		keyboardStream.onNext(KeyEvent.VK_LEFT);
		keyboardStream.onNext(KeyEvent.VK_RIGHT);
		keyboardStream.onNext(KeyEvent.VK_LEFT);
		keyboardStream.onNext(KeyEvent.VK_RIGHT);
		keyboardStream.onNext(KeyEvent.VK_B);
		keyboardStream.onNext(KeyEvent.VK_A);
		keyboardStream.onNext(KeyEvent.VK_C);
		keyboardStream.onComplete();

		List<Boolean> res = konamis.block();

		Assert.assertTrue(res.size() == 12);
		Assert.assertFalse(res.get(0));
		Assert.assertTrue(res.get(1));
		Assert.assertFalse(res.get(2));
		Assert.assertFalse(res.get(3));
		Assert.assertFalse(res.get(4));
		Assert.assertFalse(res.get(5));
		Assert.assertFalse(res.get(6));
		Assert.assertFalse(res.get(7));
		Assert.assertFalse(res.get(8));
		Assert.assertFalse(res.get(9));
		Assert.assertFalse(res.get(10));
		Assert.assertFalse(res.get(11));
	}

	@Test(timeout = TIMEOUT)
	public void multiplexUsingProcessors() throws Exception {

		final Flux<Integer> forkStream = Flux.just(1, 2, 3)
		                                     .log("begin-computation");
		final Flux<Integer> forkStream2 = Flux.just(1, 2, 3)
		                                      .log("begin-persistence");

		final TopicProcessor<Integer> computationEmitterProcessor =
				TopicProcessor.<Integer>builder().name("computation")
				                                 .bufferSize(BACKLOG)
				                                 .build();
		final Flux<String> computationStream = computationEmitterProcessor.map(i -> Integer.toString(i));

		final TopicProcessor<Integer> persistenceEmitterProcessor =
				TopicProcessor.<Integer>builder().name("persistence")
				                                 .bufferSize(BACKLOG)
				                                 .build();
		final Flux<String> persistenceStream = persistenceEmitterProcessor.map(i -> "done " + i);

		forkStream.subscribe(computationEmitterProcessor);
		forkStream2.subscribe(persistenceEmitterProcessor);

		final Semaphore doneSemaphore = new Semaphore(0);

		final Flux<List<String>> joinStream =
				Flux.zip(computationStream.log("log1"), persistenceStream.log("log2"), (a, b) -> Arrays.asList(a, b));

		// Method chaining doesn't compile.
		joinStream.log("log-final")
		          .subscribe(list -> println("Joined: ", list), t -> println("Join failed: ", t.getMessage()), () -> {
			          println("Join complete.");
			          doneSemaphore.release();
		          });

		doneSemaphore.acquire();

	}

	@Test
	public void nonSerializedSinkMultiProducer() throws Exception {
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .build();
		processor.shutdown();
		assertTrue(processor.isSerialized());
	}

	static final class SomeSubscriber extends BaseSubscriber<String> implements Scannable {

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}
	}

	@Test
	public void scanInner() {
		TopicProcessor<String> main = TopicProcessor.create("name", 16);
		RingBuffer.Sequence sequence = RingBuffer.newSequence(123);
		CoreSubscriber<String> activated = new SomeSubscriber();

		TopicProcessor.TopicInner<String> test = new TopicProcessor.TopicInner<>(main, sequence, activated);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(activated);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123L);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		main.terminated = 1;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanInnerBufferedLargeHasIntMinValue() {
		TopicProcessor<String> main = TopicProcessor.create("name", 16);
		RingBuffer.Sequence sequence = RingBuffer.newSequence(123);
		CoreSubscriber<String> sub = new BaseSubscriber<String>() {
		};
		TopicProcessor.TopicInner<String> test = new TopicProcessor.TopicInner<>(main, sequence, sub);

		main.ringBuffer.getSequencer().cursor.set(Integer.MAX_VALUE + 5L);
		test.sequence.set(2L);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(Integer.MIN_VALUE);
		assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isEqualTo(Integer.MAX_VALUE + 3L);
	}

	@Test
	public void scanInnerBufferedSmallHasIntRealValue() {
		TopicProcessor<String> main = TopicProcessor.create("name", 16);
		RingBuffer.Sequence sequence = RingBuffer.newSequence(123);
		CoreSubscriber<String> sub = new BaseSubscriber<String>() {
		};
		TopicProcessor.TopicInner<String> test = new TopicProcessor.TopicInner<>(main, sequence, sub);

		main.ringBuffer.getSequencer().cursor.set(Integer.MAX_VALUE + 5L);
		test.sequence.set(6L);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(Integer.MAX_VALUE - 1);
		assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isEqualTo(Integer.MAX_VALUE - 1L);
	}

	@Test
	public void scanProcessor() {
		TopicProcessor<String> test = TopicProcessor.create("name", 16);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isEqualTo(subscription);

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(16);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void serializedSinkSingleProducer() throws Exception {
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(false)
		                                                                     .build();
		processor.shutdown();
		assertFalse(processor.isSerialized());
	}

	@Test
	public void shareDefaultExecutorOverrideAll() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, true, name, bufferSize, waitStrategy, autoCancel, null, null);
	}

	@Test
	public void shareOverrideAll() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		ExecutorService requestTaskExecutor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .requestTaskExecutor(requestTaskExecutor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, true, null, bufferSize, waitStrategy, autoCancel, executor, requestTaskExecutor);
	}

	@Test
	public void shareOverrideAutoCancel() {
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, true, null, null, null, autoCancel, null, null);
	}

	@Test
	public void shareOverrideExecutor() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .build();
		assertProcessor(processor, true, null, null, null, null, executor, null);
	}

	@Test
	public void shareOverrideExecutorAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, true, null, null, null, autoCancel, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSize() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .build();
		assertProcessor(processor, true, null, bufferSize, null, null, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSizeAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, true, null, bufferSize, null, autoCancel, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSizeWaitStrategy() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .build();
		assertProcessor(processor, true, null, bufferSize, waitStrategy, null, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSizeWaitStrategyAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .executor(executor)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .autoCancel(autoCancel)
		                                                                     .build();
		assertProcessor(processor, true, null, bufferSize, waitStrategy, autoCancel, executor, null);
	}

	@Test
	public void shareOverrideNameBufferSize() {
		String name = "nameOverride";
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .build();
		assertProcessor(processor, true, name, bufferSize, null, null, null, null);
	}

	@Test
	public void shareOverrideNameBufferSizeWaitStrategy() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.<Integer>builder().share(true)
		                                                                     .name(name)
		                                                                     .bufferSize(bufferSize)
		                                                                     .waitStrategy(waitStrategy)
		                                                                     .build();
		assertProcessor(processor, true, name, bufferSize, waitStrategy, null, null, null);
	}

	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	@Ignore
	public void testBufferSize1Created() throws Exception {
		TopicProcessor<String> broadcast = TopicProcessor.<String>builder().name("share-name")
		                                                                   .bufferSize(1)
		                                                                   .autoCancel(true)
		                                                                   .build();

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		FluxSink<String> sink = broadcast.sink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}
		sink.next("data");

		assertThat(latch.await(4, TimeUnit.SECONDS)).overridingErrorMessage("Data not received")
		                                            .isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	@Ignore
	public void testBufferSize1Shared() throws Exception {
		TopicProcessor<String> broadcast = TopicProcessor.<String>builder().name("share-name")
		                                                                   .bufferSize(1)
		                                                                   .autoCancel(true)
		                                                                   .share(true)
		                                                                   .build();

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		FluxSink<String> sink = broadcast.sink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}
		sink.next("data");

		assertThat(latch.await(4, TimeUnit.SECONDS)).overridingErrorMessage("Data not received")
		                                            .isTrue();
	}

	@Test
	public void testCustomRequestTaskThreadName() {
		String expectedName = "topicProcessorRequestTaskCreate";
		//NOTE: the below single executor should not be used usually as requestTask assumes it immediately gets executed
		ExecutorService customTaskExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, expectedName));
		TopicProcessor<Object> processor = TopicProcessor.builder()
		                                                 .executor(Executors.newCachedThreadPool())
		                                                 .requestTaskExecutor(customTaskExecutor)
		                                                 .bufferSize(8)
		                                                 .waitStrategy(WaitStrategy.liteBlocking())
		                                                 .autoCancel(true)
		                                                 .build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		customTaskExecutor.shutdownNow();
		processor.forceShutdown();

		Condition<Thread> customRequestTaskThread =
				new Condition<>(thread -> thread != null && expectedName.equals(thread.getName()),
						"a thread named \"%s\"",
						expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}

	@Test
	public void testCustomRequestTaskThreadShare() {
		String expectedName = "topicProcessorRequestTaskShare";
		//NOTE: the below single executor should not be used usually as requestTask assumes it immediately gets executed
		ExecutorService customTaskExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, expectedName));

		TopicProcessor<Object> processor = TopicProcessor.builder()
		                                                 .share(true)
		                                                 .executor(Executors.newCachedThreadPool())
		                                                 .requestTaskExecutor(customTaskExecutor)
		                                                 .bufferSize(8)
		                                                 .waitStrategy(WaitStrategy.liteBlocking())
		                                                 .autoCancel(true)
		                                                 .build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		customTaskExecutor.shutdownNow();
		processor.forceShutdown();

		Condition<Thread> customRequestTaskThread =
				new Condition<>(thread -> thread != null && expectedName.equals(thread.getName()),
						"a thread named \"%s\"",
						expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}

	@Test
	public void testDefaultRequestTaskThreadName() {
		String mainName = "topicProcessorRequestTask";
		String expectedName = mainName + "[request-task]";

		TopicProcessor<Object> processor = TopicProcessor.builder()
		                                                 .name(mainName)
		                                                 .bufferSize(8)
		                                                 .build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		processor.forceShutdown();

		Condition<Thread> defaultRequestTaskThread =
				new Condition<>(thread -> thread != null && expectedName.equals(thread.getName()),
						"a thread named \"%s\"",
						expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, defaultRequestTaskThread);
	}

	@Test
	public void testForceShutdownAfterShutdown() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.<String>builder().name("processor")
		                                                                   .bufferSize(4)
		                                                                   .build();
		Publisher<String> publisher = Flux.fromArray(new String[]{"1", "2", "3", "4", "5"});
		publisher.subscribe(processor);

		StepVerifier.create(processor, 0)
		            .thenRequest(1)
		            .expectNext("1")
		            .thenAwait(Duration.ofMillis(250))
		            .then(() -> processor.shutdown())
		            .then(() -> assertFalse(processor.awaitAndShutdown(Duration.ofMillis(400))))
		            .then(() -> processor.forceShutdown())
		            .then(() -> assertTrue(processor.awaitAndShutdown(Duration.ofMillis(400))))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void testForceShutdownWhileWaitingForInitialRequest() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.<String>builder().name("processor")
		                                                                   .bufferSize(4)
		                                                                   .build();
		Publisher<String> publisher = new CappedPublisher(2);
		publisher.subscribe(processor);

		processor.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {

			}
		});

		processor.forceShutdown().blockLast();

		assertTrue(processor.awaitAndShutdown(Duration.ofSeconds(5)));
	}

	@Test
	@Ignore
	public void testForceShutdownWhileWaitingForMoreData() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.<String>builder().name("processor")
		                                                                   .bufferSize(4)
		                                                                   .build();
		Publisher<String> publisher = new CappedPublisher(2);
		publisher.subscribe(processor);

		BaseSubscriber<String> subscriber = new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {

			}
		};

		processor.subscribe(subscriber);

		subscriber.request(3);

		processor.forceShutdown().blockLast();

		assertTrue(processor.awaitAndShutdown(Duration.ofSeconds(5)));
	}

	@Test
	@Ignore
	public void testForceShutdownWhileWaitingForRequest() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.<String>builder().name("processor")
		                                                                   .bufferSize(4)
		                                                                   .build();
		Publisher<String> publisher = Flux.fromArray(new String[]{"1", "2", "3", "4", "5"});
		publisher.subscribe(processor);

		BaseSubscriber<String> subscriber = new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {

			}
		};

		processor.subscribe(subscriber);

		subscriber.request(1);

		processor.forceShutdown().blockLast();

		assertTrue(processor.awaitAndShutdown(Duration.ofSeconds(1)));
	}

	@Test
	public void testShutdown() {
		for (int i = 0; i < 1000; i++) {
			TopicProcessor<?> dispatcher = TopicProcessor.<String>builder().name("rb-test-dispose")
			                                                               .bufferSize(16)
			                                                               .build();
			dispatcher.awaitAndShutdown();
		}
	}

	@Test
	public void testShutdownSuccessfulAfterAllDataIsRequested() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.<String>builder().name("processor")
		                                                                   .bufferSize(4)
		                                                                   .build();
		Publisher<String> publisher = Flux.fromArray(new String[]{"1", "2", "3", "4", "5"});
		publisher.subscribe(processor);

		BaseSubscriber<String> subscriber = new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {

			}
		};

		processor.subscribe(subscriber);

		subscriber.request(1);

		Thread.sleep(250);

		processor.shutdown();

		assertFalse(processor.awaitAndShutdown(Duration.ofMillis(250)));

		subscriber.request(4);

		assertTrue(processor.awaitAndShutdown(Duration.ofMillis(250)));
	}

	@Test
	public void testTopicProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		TopicProcessor<Object> processor = TopicProcessor.builder()
		                                                 .name("testProcessor")
		                                                 .bufferSize(TEST_BUFFER_SIZE)
		                                                 .build();

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	private void assertProcessor(TopicProcessor<Integer> processor,
			boolean shared,
			@Nullable String name,
			@Nullable Integer bufferSize,
			@Nullable WaitStrategy waitStrategy,
			@Nullable Boolean autoCancel,
			@Nullable ExecutorService executor,
			@Nullable ExecutorService requestTaskExecutor) {

		String expectedName = name != null ? name : TopicProcessor.class.getSimpleName();
		int expectedBufferSize = bufferSize != null ? bufferSize : Queues.SMALL_BUFFER_SIZE;
		boolean expectedAutoCancel = autoCancel != null ? autoCancel : true;
		WaitStrategy expectedWaitStrategy =
				waitStrategy != null ? waitStrategy : WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS);
		Class<?> sequencerClass = shared ? MultiProducerRingBuffer.class : SingleProducerSequencer.class;

		assertEquals(expectedName, processor.name);
		assertEquals(expectedBufferSize, processor.getBufferSize());
		assertEquals(expectedAutoCancel, processor.autoCancel);
		assertEquals(expectedWaitStrategy.getClass(), processor.ringBuffer.getSequencer().waitStrategy.getClass());
		assertEquals(sequencerClass,
				processor.ringBuffer.getSequencer()
				                    .getClass());
		if (executor != null) {
			assertEquals(executor, processor.executor);
		}
		if (requestTaskExecutor != null) {
			assertEquals(requestTaskExecutor, processor.requestTaskExecutor);
		}
	}

	/**
	 * Publishes {@link #nItems} data items in total after that any subscription.request is no-op.
	 */
	static class CappedPublisher implements Publisher<String> {

		private final int nItems;
		private Subscriber<? super String> subscriber;

		public CappedPublisher(int nItems) {
			this.nItems = nItems;
		}

		@Override
		public void subscribe(Subscriber<? super String> s) {
			subscriber = s;
			s.onSubscribe(new Subscription() {

				private int requested;

				@Override
				public void cancel() {
				}

				@Override
				public void request(long n) {
					long limit = Math.min(n, nItems - requested);

					for (int i = 0; i < limit; i++) {
						subscriber.onNext("" + i);
					}

					requested += limit;
				}
			});
		}

	}
	private static final long TIMEOUT = 10_000;
	private static final int BACKLOG = 1024;

	static CoreSubscriber<String> sub(String name, CountDownLatch latch) {
		return new CoreSubscriber<String>() {
			Subscription s;

			@Override
			public void onComplete() {
				//latch.countDown()
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onNext(String o) {
				latch.countDown();
				s.request(1);
			}

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}
		};
	}
}
