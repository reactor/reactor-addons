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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

public class BatchedAsyncFileChannelReaderFlux extends FileFlux {

	private final Callable<AsynchronousFileChannel> asynchronousFileChannelCallable;
	private final int                               bufferCapacity;
	private final int                               batchSize;

	BatchedAsyncFileChannelReaderFlux(Callable<AsynchronousFileChannel> asynchronousFileChannelCallable,
									  int bufferCapacity,
									  int batchSize) {
		this.asynchronousFileChannelCallable = asynchronousFileChannelCallable;
		this.bufferCapacity = bufferCapacity;
		this.batchSize = batchSize;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		try {
			if (actual instanceof Fuseable.ConditionalSubscriber) {
				actual.onSubscribe(new ConditionalFileReaderSubscription(actual,
						asynchronousFileChannelCallable,
						bufferCapacity,
						batchSize));
			}
			else {
				actual.onSubscribe(new FileReaderSubscription(actual,
						asynchronousFileChannelCallable,
						bufferCapacity,
						batchSize));
			}
		}
		catch (Exception e) {
			Operators.error(actual, e);
		}
	}

	static abstract class AbstractFileReaderSubscription
			implements Scannable, Subscription {

		final CoreSubscriber<? super ByteBuffer> actual;

		final AsynchronousFileChannel                      channel;
		final PriorityBlockingQueue<PrioritizedByteBuffer> queue;
		final int                                          capacity;
		final int                                          batchSize;

		volatile boolean cancelled;

		volatile     int                                                       terminated;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
						"terminated");

		volatile     boolean                                                       done;

		volatile     long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription>
		                  REQUESTED = AtomicLongFieldUpdater.newUpdater(
				BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
				"requested");

		volatile     long                                                   position;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AbstractFileReaderSubscription> POSITION =
				AtomicLongFieldUpdater.newUpdater(BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
						"position");

		volatile     long                                                   schedules;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AbstractFileReaderSubscription> SCHEDULES =
				AtomicLongFieldUpdater.newUpdater(BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
						"schedules");

		volatile     int                                                   wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
						"wip");

		AbstractFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<AsynchronousFileChannel> asynchronousFileChannelCallable,
				int capacity,
				int batchSize) throws Exception {
			this.actual = actual;
			this.capacity = capacity;
			this.batchSize = batchSize;
			this.queue = new PriorityBlockingQueue<>(batchSize * 4);
			this.channel = asynchronousFileChannelCallable.call();
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
				doError(actual, e);
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (Operators.addCap(REQUESTED, this, n) == 0) {
					if (!isCancelledOrTerminated()) {
						if(queue.isEmpty()) {
							trySchedule();
						} else {
							drain();
						}
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

		void trySchedule() {
			long position = SCHEDULES.getAndAdd(this, batchSize);

			for (int i = 0; i < batchSize; i++) {
				ByteBuffer buffer = ByteBuffer.allocate(capacity);
				channel.read(
						buffer,
						position * capacity + capacity * i,
						buffer,
						new Handler(position + i)
				);
			}
		}

		abstract void drain();

		final class Handler implements CompletionHandler<Integer, ByteBuffer> {

			private final long priority;

			Handler(long priority) {
				this.priority = priority;
			}

			@Override
			public void completed(Integer read, ByteBuffer buffer) {
				if (read != -1) {
					if (read != capacity) {
						queue.add(new PrioritizedByteBuffer(
								priority,
								ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read)),
								true));
						done = true;
					}
					else {
						queue.add(new PrioritizedByteBuffer(
								priority,
								buffer
						));
					}
				} else {
					queue.add(new PrioritizedByteBuffer(
							priority,
							buffer,
							true,
							true
					));
					done = true;
				}

				drain();
			}

			@Override
			public void failed(Throwable exc, ByteBuffer attachment) {
				if(done || isCancelledOrTerminated()) {
					return;
				}

				doError(actual, exc);
			}
		}
	}

	static final class FileReaderSubscription extends AbstractFileReaderSubscription {

		FileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<AsynchronousFileChannel> asynchronousFileChannelCallable,
				int capacity,
				int batchSize) throws Exception {
			super(actual, asynchronousFileChannelCallable, capacity, batchSize);
		}

		@Override
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final CoreSubscriber<? super ByteBuffer> s = actual;
			int missed = 1;
			long p = position;
			long n = requested;
			boolean scheduled = false;

			for (;;) {
				long e = 0;

				main: for (;;) {
					while (e != n) {
						if (cancelled) {
							return;
						}

						PrioritizedByteBuffer next = queue.peek();

						if (null != next && p + e == next.priority) {
							if(!next.empty) {
								s.onNext(next.buffer);
							}

							queue.poll();
							scheduled = false;
							e++;

							if(next.terminal) {
								if(cancelled) {
									return;
								}

								doComplete(s);
								return;
							}
						}
						else {
							p = POSITION.addAndGet(this, e);
							n = REQUESTED.addAndGet(this, -e);

							if(next == null && !scheduled && !done && !cancelled) {
								trySchedule();
								scheduled = true;
							}

							break main;
						}
					}

					n = requested;

					if (n == e) {
						p = POSITION.addAndGet(this, e);
						n = REQUESTED.addAndGet(this, -e);
						e = 0L;

						if (n == 0L) {
							break;
						}
					}
				}

				int w = wip;
				if (missed == w) {
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}
	}

	static final class ConditionalFileReaderSubscription extends
	                                               AbstractFileReaderSubscription {

		ConditionalFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<AsynchronousFileChannel> asynchronousFileChannelCallable,
				int capacity,
				int batchSize) throws Exception {
			super(actual, asynchronousFileChannelCallable, capacity, batchSize);
		}

		@Override
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final CoreSubscriber<? super ByteBuffer> s = actual;
			final Fuseable.ConditionalSubscriber<? super  ByteBuffer> c = (Fuseable.ConditionalSubscriber<? super ByteBuffer>) s;
			int missed = 1;
			long p = position;
			long n = requested;
			boolean scheduled = false;

			for (;;) {
				long e = 0;

				main: for (;;) {
					while (e != n) {
						if (cancelled) {
							return;
						}

						PrioritizedByteBuffer next = queue.peek();
						boolean b = false;

						if (null != next && p + e == next.priority) {
							if(!next.empty) {
								b = c.tryOnNext(next.buffer);
							}

							queue.poll();
							scheduled = false;

							if (b) {
								e++;
							}

							if(next.terminal) {
								if(cancelled) {
									return;
								}

								doComplete(s);
								return;
							}
						}
						else {
							p = POSITION.addAndGet(this, e);
							n = REQUESTED.addAndGet(this, -e);

							if(next == null && !scheduled && !done && !cancelled) {
								trySchedule();
								scheduled = true;
							}

							break main;
						}
					}

					n = requested;

					if (n == e) {
						p = POSITION.addAndGet(this, e);
						n = REQUESTED.addAndGet(this, -e);
						e = 0L;

						if (n == 0L) {
							break;
						}
					}
				}

				int w = wip;
				if (missed == w) {
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}
	}

	static final class PrioritizedByteBuffer implements
	                                         Comparable<PrioritizedByteBuffer> {

		final long       priority;
		final ByteBuffer buffer;
		final boolean    terminal;
		final boolean    empty;

		PrioritizedByteBuffer(long priority, ByteBuffer buffer) {
			this(priority, buffer, false, false);
		}

		PrioritizedByteBuffer(long priority, ByteBuffer buffer, boolean terminal) {
			this(priority, buffer, terminal, false);
		}

		PrioritizedByteBuffer(long priority,
				ByteBuffer buffer,
				boolean terminal,
				boolean empty) {
			this.priority = priority;
			this.buffer = buffer;
			this.terminal = terminal;
			this.empty = empty;
		}

		@Override
		public int compareTo(@NotNull PrioritizedByteBuffer o) {
			return Long.compare(priority, o.priority);
		}
	}
}
