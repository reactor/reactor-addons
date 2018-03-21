package reactor.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

public class AsyncFileChannelReaderFlux extends FileFlux {

	private final Path      file;
	private final int       bufferCapacity;

	AsyncFileChannelReaderFlux(Path file, int bufferCapacity) {
		this.file = file;
		this.bufferCapacity = bufferCapacity;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		try {
			if (actual instanceof Fuseable.ConditionalSubscriber) {
				actual.onSubscribe(new ConditionalFileReaderSubscription(actual,
						file,
						bufferCapacity));
			}
			else {
				actual.onSubscribe(new FileReaderSubscription(actual,
						file,
						bufferCapacity));
			}
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
				Path file,
				int capacity) throws IOException {
			this.actual = actual;
			this.capacity = capacity;
			this.channel = AsynchronousFileChannel.open(file, Collections.emptySet(),
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
				Path file,
				int capacity) throws IOException {
			super(actual, file, capacity);
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
									r = REQUESTED.addAndGet(FileReaderSubscription.this, -next);
									next = 0;
									if (r == 0L) {
										POSITION.set(FileReaderSubscription.this, nextP);
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
				Path file,
				int capacity) throws IOException {
			super(actual, file, capacity);
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
									r = REQUESTED.addAndGet(ConditionalFileReaderSubscription.this, -next);
									next = 0;
									if (r == 0L) {
										POSITION.set(ConditionalFileReaderSubscription.this, nextP);
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
