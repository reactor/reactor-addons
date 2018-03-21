package reactor.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

public class BatchedAsyncFileChannelReaderFlux extends FileFlux {

	private final Path file;
	private final int  bufferCapacity;
	private final int  batchSize;

	BatchedAsyncFileChannelReaderFlux(Path file, int bufferCapacity, int batchSize) {
		this.file = file;
		this.bufferCapacity = bufferCapacity;
		this.batchSize = batchSize;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		try {
//			if (actual instanceof Fuseable.ConditionalSubscriber) {
//				actual.onSubscribe(new ConditionalFileReaderSubscription(actual,
//						file,
//						bufferCapacity));
//			}
//			else {
				actual.onSubscribe(new FileReaderSubscription(actual,
						file,
						bufferCapacity, batchSize));
//			}
		}
		catch (IOException e) {
			Operators.error(actual, e);
		}
	}

	static abstract class AbstractFileReaderSubscription
			implements Scannable, Subscription {

		final CoreSubscriber<? super ByteBuffer> actual;

		final AsynchronousFileChannel channel;
		final int                     capacity;
		final int                     parallelization;

		final PriorityBlockingQueue<PrioritizedByteBuffer>   queue;

		volatile boolean cancelled;

		volatile     int                                                       terminated;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription>
		                                                                       TERMINATED =
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

		volatile     int                                                   pending;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> PENDING =
				AtomicIntegerFieldUpdater.newUpdater(BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
						"pending");

		volatile     int                                                   wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BatchedAsyncFileChannelReaderFlux.AbstractFileReaderSubscription.class,
						"wip");


		AbstractFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				int parallelization) throws IOException {
			this.actual = actual;
			this.capacity = capacity;
			this.parallelization = parallelization;
			this.queue = new PriorityBlockingQueue<>(parallelization * 2);
			this.channel = AsynchronousFileChannel.open(file,
					Collections.emptySet(),
					ForkJoinPool.commonPool());
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

			try {
				channel.close();
				cancelled = true;
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
							trySchedule(position, n);
						} else {
							drain(position, n, pending);
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

		void trySchedule(long position, long n) {
			position = (long) Math.ceil((double) position / (double) parallelization) * parallelization;
			PENDING.addAndGet(this, parallelization);

			for (int i = 0; i < parallelization; i++) {
				ByteBuffer buffer = ByteBuffer.allocate(capacity);
				channel.read(
						buffer,
						position * capacity + capacity * i,
						buffer,
						new Handler(position, position + i, n)
				);
			}
		}

		abstract void drain(long p, long n, int pending);

		final class Handler implements CompletionHandler<Integer, ByteBuffer> {

			private final long position;
			private final long priority;
			private final long requested;

			Handler(long position, long priority, long requested) {
				this.position = position;
				this.priority = priority;
				this.requested = requested;
			}

			@Override
			public void completed(Integer read, ByteBuffer buffer) {
				if (read != -1) {
					if (read != capacity) {
						queue.add(new PrioritizedByteBuffer(
								priority,
								ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read))
						));
						done = true;
					}
					else {
						queue.add(new PrioritizedByteBuffer(
								priority,
								buffer
						));
					}
				} else {
					done = true;
				}

				drain(position, requested, pending);
				PENDING.decrementAndGet(AbstractFileReaderSubscription.this);
			}

			@Override
			public void failed(Throwable exc, ByteBuffer attachment) {
				if(isCancelledOrTerminated()) {
					return;
				}

				doError(actual, exc);
			}
		}
	}

	static final class FileReaderSubscription extends AbstractFileReaderSubscription {

		FileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				int parallelization) throws IOException {
			super(actual, file, capacity, parallelization);
		}

		@Override
		void drain(long p, long n, int r) {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			long e = 0;
			int missed = 1;

			for (;;) {
				main: for (;;) {
					while (e != n) {
						if (isCancelledOrTerminated()) {
							return;
						}

						PrioritizedByteBuffer item = queue.peek();
						if (item != null && item.priority == p + e) {
							actual.onNext(item.byteBuffer);
							queue.poll();
							e++;
						}
						else {
							p = POSITION.addAndGet(this, e);
							n = REQUESTED.addAndGet(this, -e);
							e = 0L;

							if(item == null && !done && !isCancelledOrTerminated()) {
								trySchedule(p, n);
							}

							break main;
						}
					}

					n = requested;

					if (n == e) {
						n = REQUESTED.addAndGet(this, -e);
						p = POSITION.addAndGet(this, e);
						e = 0L;

						if (n == 0L) {
							break;
						}
					}
				}

				if (isCancelledOrTerminated()) {
					return;
				}

				if(done && queue.isEmpty() && r == 1) {
					doComplete(actual);
					return;
				}



				r = pending;
				n = requested;

				if(!queue.isEmpty()) {
					continue;
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
					r = pending;
				}
			}
		}
	}

	static final class PrioritizedByteBuffer implements
	                                         Comparable<PrioritizedByteBuffer> {

		final long       priority;
		final ByteBuffer byteBuffer;

		PrioritizedByteBuffer(long priority, ByteBuffer buffer) {
			this.priority = priority;
			byteBuffer = buffer;
		}

		@Override
		public int compareTo(@NotNull PrioritizedByteBuffer o) {
			return Long.compare(priority, o.priority);
		}
	}



	/*
	static final class ConditionalFileReaderSubscription
			extends AbstractFileReaderSubscription {

		ConditionalFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity) throws IOException {
			super(actual, file, capacity, parallelization);
		}

		@Override
		@SuppressWarnings("unchecked")
		void run(long n, long i, long position) {
			final Fuseable.ConditionalSubscriber<? super ByteBuffer> c =
					(Fuseable.ConditionalSubscriber<? super ByteBuffer>) actual;
			ByteBuffer buffer = ByteBuffer.allocate(capacity);
			channel.read(buffer,
					position,
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
									b =
											c.tryOnNext(ByteBuffer.wrap(Arrays.copyOf(
													buffer.array(),
													read)));
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

							if (b) {
								next++;
							}

							if (n != next) {
								run(n, next, nextP);
							}
							else {
								long r = requested;

								if (r == next) {
									r = REQUESTED.addAndGet(
											ConditionalFileReaderSubscription.this,
											-next);
									next = 0;
									if (r == 0L) {
										POSITION.set(ConditionalFileReaderSubscription.this,
												nextP);
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
	}*/
}
