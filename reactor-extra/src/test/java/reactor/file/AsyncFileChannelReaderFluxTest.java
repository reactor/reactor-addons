/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Assert;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class AsyncFileChannelReaderFluxTest extends PublisherVerification<ByteBuffer> {

	public static final Function<Path, Callable<AsynchronousFileChannel>> CREATOR = ptf -> () ->
			AsynchronousFileChannel.open(ptf, Collections.emptySet(), ForkJoinPool.commonPool());

	public static final String EMPTY_FILE = new File(ClassLoader.getSystemResource("empty.txt")
	                                                            .getFile())
															    .getPath();
	public static final String SHAKESPEARE_FILE = new File(ClassLoader.getSystemResource("shakespeare.txt")
	                                                                  .getFile())
																	  .getPath();
	public static final String DEFAULT_FILE = new File(ClassLoader.getSystemResource("default.txt")
	                                                              .getFile())
																  .getPath();
	public static final String FILE_CONTENT =
			"1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n" + "7\n" + "8\n" + "9\n" + "10 11 12";

	public AsyncFileChannelReaderFluxTest() {
		super(new TestEnvironment());
	}

	@Override
	public Publisher<ByteBuffer> createPublisher(long elements) {
		Path shakespeareFilePath = Paths.get(SHAKESPEARE_FILE);
		Path emptyFilePath = Paths.get(EMPTY_FILE);
		Path defaultFilePath = Paths.get(DEFAULT_FILE);

		return elements == 0 ? new AsyncFileChannelReaderFlux(CREATOR.apply(emptyFilePath), 1024) :
				elements > 26 ?
				new AsyncFileChannelReaderFlux(CREATOR.apply(shakespeareFilePath), 1024) :
				new AsyncFileChannelReaderFlux(CREATOR.apply(defaultFilePath), (int) Math.ceil(26d / (double) elements));
	}

	@Override
	public Publisher<ByteBuffer> createFailedPublisher() {
		Path wrongFilePath = Paths.get("./src/test/resources/none.txt");
		return new AsyncFileChannelReaderFlux(CREATOR.apply(wrongFilePath), 2);
	}

	@org.junit.Test
	public void empty(){

	}

	@Test
	public void shouldBeAbleToReadFileInFastPath() {
		Path path = Paths.get(DEFAULT_FILE);

		Mono<String> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 1024)
		                                .reduce(new StringBuffer(),
				                                (sb, bb) -> sb.append(new String(bb.array())))
		                                .map(StringBuffer::toString);

		StepVerifier.create(fileFlux)
		            .expectSubscription()
		            .expectNext(FILE_CONTENT)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void shouldBeAbleToReadFileInSlowPath() {
		Path path = Paths.get(DEFAULT_FILE);

		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 8);

		StepVerifier.create(fileFlux, 1)
		            .expectSubscription()
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectNextCount(1)
		            .thenRequest(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void shouldBeAbleToHandleError() {
		Path path = Paths.get("./src/test/resources/file.t");

		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 8);

		StepVerifier.create(fileFlux)
		            .expectSubscription()
		            .expectError()
		            .verify();
	}

	@Test
	public void shouldNotFailOnConcurrentRequests() throws InterruptedException {
		Path path = Paths.get(DEFAULT_FILE);

		TestSubscriber actual = new TestSubscriber();
		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 1024);

		fileFlux.subscribe(actual);

		ForkJoinTask<?> submit1 = ForkJoinPool.commonPool()
		                                      .submit(() -> actual.request(1));
		ForkJoinTask<?> submit2 = ForkJoinPool.commonPool()
		                                      .submit(() -> actual.request(1));
		actual.request(1);
		submit1.join();
		submit2.join();
		actual.awaitCompletion();
	}

	@Test
	public void shouldNotFailOnConcurrentRequestsAndError()
			throws IOException, InterruptedException {
		Path path = Paths.get(DEFAULT_FILE);

		TestSubscriber actual = new TestSubscriber();
		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 1024);

		fileFlux.subscribe(actual);

		ForkJoinTask<?> submit1 = ForkJoinPool.commonPool()
		                                      .submit(() -> actual.request(1));
		actual.s.channel.close();
		ForkJoinTask<?> submit2 = ForkJoinPool.commonPool()
		                                      .submit(() -> actual.request(1));
		actual.request(1);
		submit1.join();
		submit2.join();
		actual.awaitError();
	}

	@Test
	public void shouldEmitZeroElementsOnAlwaysFalseConditionalSubscriber()
			throws InterruptedException {
		Path path = Paths.get(DEFAULT_FILE);

		TestSubscriber actual = new ConditionalTestSubscriber(false);
		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 1024);

		fileFlux.subscribe(actual);

		actual.request(Long.MAX_VALUE);

		actual.awaitCompletion();
		actual.assertInvokedTimes(0);
	}

	@Test
	public void shouldCloseChannelOnCompletion() throws InterruptedException {
		Path path = Paths.get(DEFAULT_FILE);

		TestSubscriber actual = new TestSubscriber();
		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 1024);

		fileFlux.subscribe(actual);

		actual.request(1);

		actual.awaitCompletion();
		actual.assertCompleted();
		Assert.assertFalse(actual.s.channel.isOpen());
	}

	@Test
	public void shouldCloseChannelOnError() {
		Path path = Paths.get(DEFAULT_FILE);

		TestSubscriber actual = new TestSubscriber();
		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 1024);

		fileFlux.subscribe(actual);
		actual.s.doError(actual, new NullPointerException());

		actual.assertError();
		Assert.assertFalse(actual.s.channel.isOpen());
	}

	@Test
	public void shouldCloseChannelOnCancel() {
		Path path = Paths.get(SHAKESPEARE_FILE);

		TestSubscriber actual = new TestSubscriber();
		Flux<ByteBuffer> fileFlux = new AsyncFileChannelReaderFlux(CREATOR.apply(path), 2);

		fileFlux.subscribe(actual);
		actual.request(Long.MAX_VALUE);
		actual.cancel();

		Assert.assertFalse(actual.s.channel.isOpen());
	}

	static class TestSubscriber implements CoreSubscriber<ByteBuffer> {

		volatile AsyncFileChannelReaderFlux.AbstractFileReaderSubscription s;
		volatile Throwable                                            throwable;
		volatile boolean                                              done;
		volatile LinkedBlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<>();

		@Override
		public void onSubscribe(Subscription s) {

			this.s = (AsyncFileChannelReaderFlux.AbstractFileReaderSubscription) s;
		}

		@Override
		public void onNext(ByteBuffer buffer) {
			queue.offer(buffer);
		}

		@Override
		public void onError(Throwable throwable) {
			Assert.assertNull(this.throwable);

			this.throwable = throwable;
			synchronized (this) {
				this.notify();
			}
		}

		@Override
		public void onComplete() {
			Assert.assertFalse(done);

			done = true;
			synchronized (this) {
				this.notify();
			}
		}

		public void request(long n) {
			s.request(n);
		}

		public void cancel() {
			s.cancel();
		}

		void awaitInvokedTimes(int times) throws InterruptedException {
			for (int i = 0; i < times; i++) {
				queue.poll(1, TimeUnit.SECONDS);
			}
		}

		void assertInvokedTimes(int times) {
			Assert.assertEquals("Unexpected invocation count; Actual: [" + queue.size() + "]," + " but " + "Expected: [" + times + "]",
					times,
					queue.size());
		}

		synchronized void awaitCompletion() throws InterruptedException {
			if (!done) {
				this.wait();
			}
		}

		void assertCompleted() {
			Assert.assertTrue("Expected completion", done);
		}

		synchronized void awaitError() throws InterruptedException {
			if (throwable == null) {
				this.wait();
			}
		}

		void assertError() {
			Assert.assertNotNull(throwable);
		}
	}

	static class ConditionalTestSubscriber extends TestSubscriber
			implements Fuseable.ConditionalSubscriber<ByteBuffer> {

		final boolean shouldCallOnNext;

		ConditionalTestSubscriber(boolean next) {
			shouldCallOnNext = next;
		}

		@Override
		public boolean tryOnNext(ByteBuffer buffer) {
			if (shouldCallOnNext) {
				onNext(buffer);
				return true;
			}
			else {
				return false;
			}
		}
	}
}
