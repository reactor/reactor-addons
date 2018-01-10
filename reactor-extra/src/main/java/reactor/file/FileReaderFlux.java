package reactor.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

public class FileReaderFlux extends Flux<ByteBuffer> {

	private final Path      file;
	private final int       bufferCapacity;
	private final Scheduler scheduler;

	FileReaderFlux(Path file, int bufferCapacity, Scheduler scheduler) {
		this.file = file;
		this.bufferCapacity = bufferCapacity;
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		actual.onSubscribe(new FileReaderSubscription(
				actual,
				file,
				bufferCapacity,
				scheduler.createWorker()
		));
	}

	static abstract class AbstractFileReaderSubscription
			implements Scannable, Subscription, Runnable {

		final CoreSubscriber<? super ByteBuffer> actual;

		final FileChannel      channel;
		final Path             file;
		final int              capacity;
		final Scheduler.Worker worker;

		Throwable error;

		volatile boolean cancelled;
		volatile boolean done;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<FileReaderFlux.AbstractFileReaderSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FileReaderFlux.AbstractFileReaderSubscription.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FileReaderFlux.AbstractFileReaderSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FileReaderFlux.AbstractFileReaderSubscription.class, "wip");

		AbstractFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				Scheduler.Worker worker) {
			this.actual = actual;
			this.file = file;
			this.capacity = capacity;
			this.worker = worker;

			FileChannel channel;

			try {
				channel = FileChannel.open(file);
			}
			catch (IOException e) {
				channel = null;
			}

			this.channel = channel;
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
			if (cancelled) {
				return;
			}

			cancelled = true;
			worker.dispose();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				trySchedule();
			}
		}

		@Override
		public void run() {
			long n = requested;

			if (n == Long.MAX_VALUE) {
				fastPath();
			}
			else {
				final CoreSubscriber<? super ByteBuffer> a = actual;
				int missed = 1;

				for (; ; ) {
					slowPath();

					if (cancelled || done) {
						return;
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

		void trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				actual.onError(Operators.onRejectedExecution(ree, actual.currentContext()));
			}
		}

		void doComplete(CoreSubscriber<?> a) {
			try {
				channel.close();
				done = true;
				a.onComplete();
				worker.dispose();
			}
			catch (IOException e) {
				doError(a, e);
			}
		}

		void doError(CoreSubscriber<?> a, Throwable e) {
			try {
				a.onError(e);
				if(channel.isOpen()) {
					channel.close();
				}
			}
			catch (IOException e1) {
				Operators.onErrorDropped(e1, a.currentContext());
			}
			finally {
				worker.dispose();
			}
		}

		abstract void fastPath();

		abstract void slowPath();
	}

	static final class FileReaderSubscription extends AbstractFileReaderSubscription {

		FileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				Scheduler.Worker worker) {
			super(actual, file, capacity, worker);
		}

		@Override
		void fastPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			int read = 0;

			for(;;) {
				ByteBuffer buffer = ByteBuffer.allocate(capacity);

				try {
					read = channel.read(buffer);

					if(read > -1) {
						if(read != capacity) {
							buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read));
						}

						if (cancelled) {
							return;
						}

						s.onNext(buffer);
					}
				}
				catch (IOException e) {
					doError(s, e);
					return;
				}

				if (cancelled) {
					return;
				}

				if (read == -1) {
					doComplete(s);
				}
			}
		}

		@Override
		void slowPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			int read = 0;
			long n = requested;
			long e = 0L;

			for(;;) {

				while (e != n) {
					ByteBuffer buffer = ByteBuffer.allocate(capacity);

					try {
						read = channel.read(buffer);

						if(read > -1) {
							if(read != capacity) {
								buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read));
							}

							if (cancelled) {
								return;
							}

							s.onNext(buffer);
						}
					}
					catch (IOException t) {
						doError(s, t);
					}

					if (cancelled) {
						return;
					}

					if (read == -1) {
						doComplete(s);
						return;
					}

					e++;
				}

				n = requested;

				if (n == e) {
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0L) {
						return;
					}
					e = 0L;
				}
			}
		}
	}

}
