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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

public class AsyncFileChannelReaderFlux extends FileFlux {

	private final Callable<AsynchronousFileChannel> asynchronousFileChannelCallable;
	private final int                               bufferCapacity;

	AsyncFileChannelReaderFlux(Callable<AsynchronousFileChannel> asynchronousFileChannelCallable,
			int bufferCapacity) {
		this.asynchronousFileChannelCallable = asynchronousFileChannelCallable;
		this.bufferCapacity = bufferCapacity;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		try {
			if (actual instanceof Fuseable.ConditionalSubscriber) {
				actual.onSubscribe(new ConditionalFileReaderSubscription(actual,
						asynchronousFileChannelCallable,
						bufferCapacity));
			}
			else {
				actual.onSubscribe(new FileReaderSubscription(actual,
						asynchronousFileChannelCallable,
						bufferCapacity));
			}
		}
		catch (Exception e) {
			Operators.error(actual, e);
		}
	}

	static abstract class AbstractFileReaderSubscription
			implements Scannable, Subscription {

		final CoreSubscriber<? super ByteBuffer> actual;

		final AsynchronousFileChannel channel;
		final int                     capacity;

		volatile boolean cancelled;

		volatile int terminated;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(AbstractFileReaderSubscription.class, "terminated");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AbstractFileReaderSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(AbstractFileReaderSubscription.class, "requested");

		volatile long position;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AbstractFileReaderSubscription> POSITION =
				AtomicLongFieldUpdater.newUpdater(AbstractFileReaderSubscription.class, "position");

		AbstractFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<AsynchronousFileChannel> fileChannelCallable,
				int capacity) throws Exception {
			this.actual = actual;
			this.capacity = capacity;
			this.channel = fileChannelCallable.call();
		}

		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) {
				return actual();
			}

			return null;
		}

		public CoreSubscriber<? super ByteBuffer> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			if (isCancelledOrTerminated()) {
				return;
			}

			cancelled = true;

			try {
				channel.close();
			}
			catch (IOException e) {
				Operators.onErrorDropped(e, actual.currentContext());
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (Operators.addCap(REQUESTED, this, n) == 0) {
					if (!isCancelledOrTerminated()) {
						run(n, 0, position);
					}
				}
			}
		}

		void doComplete(CoreSubscriber<?> a) {
			try {
				channel.close();
				if (TERMINATED.compareAndSet(this, 0, 1)) {
					a.onComplete();
				}
			}
			catch (IOException e) {
				doError(a, e);
			}
		}

		void doError(CoreSubscriber<?> a, Throwable e) {
			try {
				if (channel.isOpen()) {
					channel.close();
				}
			}
			catch (IOException t) {
				Operators.onErrorDropped(t, a.currentContext());
			}
			finally {
				if (TERMINATED.compareAndSet(this, 0, 1)) {
					a.onError(e);
				}
			}
		}

		boolean isCancelledOrTerminated() {
			return cancelled || terminated == 1;
		}

		abstract void run(long n, long i, long position);
	}

	static final class FileReaderSubscription extends AbstractFileReaderSubscription {

		FileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<AsynchronousFileChannel> fileChannelCallable,
				int capacity) throws Exception {
			super(actual, fileChannelCallable, capacity);
		}

		@Override
		void run(long n, long i, long position) {
			ByteBuffer buffer = ByteBuffer.allocate(capacity);
			channel.read(buffer, position,
					null,
					new CompletionHandler<Integer, Object>() {
						@Override
						public void completed(Integer read, Object attachment) {
							if (cancelled) {
								return;
							}

							if (read != -1) {
								if (read != capacity) {
									actual.onNext(ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read)));
								}
								else {
									actual.onNext(buffer);
								}
							}

							if (cancelled) {
								return;
					 		}

							if (read < capacity) {
								doComplete(actual);
								return;
							}

							long next = i + 1;
							long nextP = position + capacity;

							if (n != next) {
								run(n, next, nextP);
							} else {
								long r = requested;

								if (r == next) {
									POSITION.set(FileReaderSubscription.this, nextP);
									r = REQUESTED.addAndGet(FileReaderSubscription.this, -next);
									next = 0;
									if (r == 0L) {
										return;
									}
								}

								run(r, next, nextP);
							}
						}

						@Override
						public void failed(Throwable exc, Object attachment) {
							if(isCancelledOrTerminated()) {
								return;
							}

							doError(actual, exc);
						}
					});
		}
	}

	static final class ConditionalFileReaderSubscription
			extends AbstractFileReaderSubscription {

		ConditionalFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<AsynchronousFileChannel> fileChannelCallable,
				int capacity) throws Exception {
			super(actual, fileChannelCallable, capacity);
		}

		@Override
		@SuppressWarnings("unchecked")
		void run(long n, long i, long position) {
			final Fuseable.ConditionalSubscriber<? super  ByteBuffer> c = (Fuseable.ConditionalSubscriber<? super ByteBuffer>) actual;
			ByteBuffer buffer = ByteBuffer.allocate(capacity);
			channel.read(buffer, position,
					null,
					new CompletionHandler<Integer, Object>() {
						@Override
						public void completed(Integer read, Object attachment) {
							if (cancelled) {
								return;
							}

							boolean b = false;

							if (read != -1) {
								if (read != capacity) {
									b = c.tryOnNext(ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read)));
								}
								else {
									b = c.tryOnNext(buffer);
								}
							}

							if (cancelled) {
								return;
							}

							if (read < capacity) {
								doComplete(actual);
								return;
							}

							long next = i;
							long nextP = position + capacity;

							if(b) {
								next++;
							}

							if (n != next) {
								run(n, next, nextP);
							} else {
								long r = requested;

								if (r == next) {
									POSITION.set(ConditionalFileReaderSubscription.this, nextP);
									r = REQUESTED.addAndGet(ConditionalFileReaderSubscription.this, -next);
									next = 0;
									if (r == 0L) {
										return;
									}
								}

								run(r, next, nextP);
							}
						}

						@Override
						public void failed(Throwable exc, Object attachment) {
							doError(actual, exc);
						}
					});
		}
	}
}
