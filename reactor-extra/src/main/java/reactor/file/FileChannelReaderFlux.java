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
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

public class FileChannelReaderFlux extends FileFlux {

	private final Path      file;
	private final int       bufferCapacity;
	private final Scheduler scheduler;

	FileChannelReaderFlux(Path file, int bufferCapacity, Scheduler scheduler) {
		this.file = file;
		this.bufferCapacity = bufferCapacity;
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		try {
			if (actual instanceof Fuseable.ConditionalSubscriber) {
				actual.onSubscribe(new ConditionalFileReaderSubscription(actual,
						file,
						bufferCapacity,
						scheduler.createWorker()));
			}
			else {
				actual.onSubscribe(new FileReaderSubscription(actual,
						file,
						bufferCapacity,
						scheduler.createWorker()));
			}
		}
		catch (IOException e) {
			Operators.error(actual, e);
		}
	}

	static abstract class AbstractFileReaderSubscription
			implements Scannable, Subscription, Runnable {

		final CoreSubscriber<? super ByteBuffer> actual;

		final FileChannel      channel;
		final int              capacity;
		final Scheduler.Worker worker;

		volatile boolean cancelled;

		volatile int terminated;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(FileChannelReaderFlux.AbstractFileReaderSubscription.class, "terminated");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<FileChannelReaderFlux.AbstractFileReaderSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FileChannelReaderFlux.AbstractFileReaderSubscription.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FileChannelReaderFlux.AbstractFileReaderSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FileChannelReaderFlux.AbstractFileReaderSubscription.class, "wip");

		AbstractFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				Scheduler.Worker worker) throws IOException {
			this.actual = actual;
			this.capacity = capacity;
			this.worker = worker;
			this.channel = FileChannel.open(file);
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

			try {
				channel.close();

				cancelled = true;
				worker.dispose();
			}
			catch (IOException e) {
				doError(actual, e);
			}
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
				int missed = 1;

				for (;;) {
					if (isCancelledOrTerminated()) {
						return;
					}

					slowPath();

					if (isCancelledOrTerminated()) {
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
				CoreSubscriber<? super ByteBuffer> actual = this.actual;
				doError(actual, Operators.onRejectedExecution(ree, actual.currentContext()));
			}
		}

		void doComplete(CoreSubscriber<?> a) {
			try {
				channel.close();
				if (TERMINATED.compareAndSet(this, 0, 1)) {
					a.onComplete();
					worker.dispose();
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
					worker.dispose();
				}
			}
		}

		boolean isCancelledOrTerminated() {
			return cancelled || terminated == 1;
		}

		abstract void fastPath();

		abstract void slowPath();
	}

	static final class FileReaderSubscription extends AbstractFileReaderSubscription {

		FileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				Scheduler.Worker worker) throws IOException {
			super(actual, file, capacity, worker);
		}

		@Override
		void fastPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			int read;

			for (;;) {
				ByteBuffer buffer = ByteBuffer.allocate(capacity);

				try {
					read = channel.read(buffer);
				}
				catch (IOException e) {
					doError(s, e);
					return;
				}

				if (read > -1) {
					if (read != capacity) {
						buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read));
					}

					s.onNext(buffer);
				}

				if (cancelled) {
					return;
				}

				if (read < capacity) {
					doComplete(s);
					return;
				}
			}
		}

		@Override
		void slowPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			int read;
			long n = requested;
			long e = 0L;

			for (;;) {

				while (e != n) {
					ByteBuffer buffer = ByteBuffer.allocate(capacity);

					try {
						read = channel.read(buffer);
					}
					catch (IOException t) {
						doError(s, t);
						return;
					}

					if (read > -1) {
						if (read != capacity) {
							buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read));
						}

						s.onNext(buffer);
					}

					if (cancelled) {
						return;
					}

					if (read < capacity) {
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

	static final class ConditionalFileReaderSubscription
			extends AbstractFileReaderSubscription {

		ConditionalFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Path file,
				int capacity,
				Scheduler.Worker worker) throws IOException {
			super(actual, file, capacity, worker);
		}

		@Override
		@SuppressWarnings("unchecked")
		void fastPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			final Fuseable.ConditionalSubscriber<? super  ByteBuffer> c = (Fuseable.ConditionalSubscriber<? super ByteBuffer>) s;
			int read;

			for (;;) {
				ByteBuffer buffer = ByteBuffer.allocate(capacity);

				try {
					read = channel.read(buffer);
				}
				catch (IOException e) {
					doError(s, e);
					return;
				}

				if (read > -1) {
					if (read != capacity) {
						buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read));
					}

					c.tryOnNext(buffer);
				}

				if (cancelled) {
					return;
				}

				if (read < capacity) {
					doComplete(s);
					return;
				}
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		void slowPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			final Fuseable.ConditionalSubscriber<? super ByteBuffer> c =
					(Fuseable.ConditionalSubscriber<? super ByteBuffer>) s;
			int read;
			long n = requested;
			long e = 0L;

			for (;;) {

				while (e != n) {
					ByteBuffer buffer = ByteBuffer.allocate(capacity);

					try {
						read = channel.read(buffer);
					}
					catch (IOException t) {
						doError(s, t);
						return;
					}

					boolean b = false;

					if (read > -1) {
						if (read != capacity) {
							buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), read));
						}

						b = c.tryOnNext(buffer);
					}

					if (cancelled) {
						return;
					}

					if (read < capacity) {
						doComplete(s);
						return;
					}

					if (b) {
						e++;
					}
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
