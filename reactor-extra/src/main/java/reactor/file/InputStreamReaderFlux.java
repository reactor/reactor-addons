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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;
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

public class InputStreamReaderFlux extends FileFlux {

	private final Callable<InputStream> inputStreamCallable;
	private final int                   bufferCapacity;
	private final Scheduler             scheduler;

	InputStreamReaderFlux(Callable<InputStream> inputStreamCallable,
			int bufferCapacity,
			Scheduler scheduler) {
		this.inputStreamCallable = inputStreamCallable;
		this.bufferCapacity = bufferCapacity;
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {
		try {
			if (actual instanceof Fuseable.ConditionalSubscriber) {
				actual.onSubscribe(new ConditionalFileReaderSubscription(actual,
						inputStreamCallable,
						bufferCapacity,
						scheduler.createWorker()));
			}
			else {
				actual.onSubscribe(new FileReaderSubscription(actual,
						inputStreamCallable,
						bufferCapacity,
						scheduler.createWorker()));
			}
		}
		catch (Exception e) {
			Operators.error(actual, e);
		}
	}

	static abstract class AbstractFileReaderSubscription
			implements Scannable, Subscription, Runnable {

		final CoreSubscriber<? super ByteBuffer> actual;

		final InputStream      inputStream;
		final int              capacity;
		final Scheduler.Worker worker;

		volatile boolean cancelled;

		volatile int terminated;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(AbstractFileReaderSubscription.class, "terminated");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AbstractFileReaderSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(AbstractFileReaderSubscription.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractFileReaderSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(AbstractFileReaderSubscription.class, "wip");

		AbstractFileReaderSubscription(CoreSubscriber<? super ByteBuffer> actual,
				Callable<InputStream> inputStreamCallable,
				int capacity,
				Scheduler.Worker worker) throws Exception {
			this.actual = actual;
			this.capacity = capacity;
			this.worker = worker;
			this.inputStream = inputStreamCallable.call();
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
				inputStream.close();
			}
			catch (IOException e) {
				Operators.onErrorDropped(e, actual.currentContext());
			} finally {
				worker.dispose();
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
				inputStream.close();
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
				inputStream.close();
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
				Callable<InputStream> inputStreamCallable,
				int capacity,
				Scheduler.Worker worker) throws Exception {
			super(actual, inputStreamCallable, capacity, worker);
		}

		@Override
		void fastPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			int read;

			for (;;) {
				byte[] bytes = new byte[capacity];

				try {
					read = inputStream.read(bytes);
				}
				catch (IOException e) {
					doError(s, e);
					return;
				}

				if (read > -1) {
					ByteBuffer buffer;

					if (read != capacity) {
						buffer = ByteBuffer.wrap(Arrays.copyOf(bytes, read));
					} else {
						buffer = ByteBuffer.wrap(bytes);
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
					byte[] bytes = new byte[capacity];

					try {
						read = inputStream.read(bytes);
					}
					catch (IOException t) {
						doError(s, t);
						return;
					}

					if (read > -1) {
						ByteBuffer buffer;

						if (read != capacity) {
							buffer = ByteBuffer.wrap(Arrays.copyOf(bytes, read));
						} else {
							buffer = ByteBuffer.wrap(bytes);
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
				Callable<InputStream> inputStreamCallable,
				int capacity,
				Scheduler.Worker worker) throws Exception {
			super(actual, inputStreamCallable, capacity, worker);
		}

		@Override
		@SuppressWarnings("unchecked")
		void fastPath() {
			final CoreSubscriber<? super ByteBuffer> s = actual;
			final Fuseable.ConditionalSubscriber<? super  ByteBuffer> c = (Fuseable.ConditionalSubscriber<? super ByteBuffer>) s;
			int read;

			for (;;) {
				byte[] bytes = new byte[capacity];

				try {
					read = inputStream.read(bytes);
				}
				catch (IOException e) {
					doError(s, e);
					return;
				}

				if (read > -1) {
					ByteBuffer buffer;

					if (read != capacity) {
						buffer = ByteBuffer.wrap(Arrays.copyOf(bytes, read));
					} else {
						buffer = ByteBuffer.wrap(bytes);
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
					byte[] bytes = new byte[capacity];

					try {
						read = inputStream.read(bytes);
					}
					catch (IOException t) {
						doError(s, t);
						return;
					}

					boolean b = false;

					if (read > -1) {
						ByteBuffer buffer;

						if (read != capacity) {
							buffer = ByteBuffer.wrap(Arrays.copyOf(bytes, read));
						} else {
							buffer = ByteBuffer.wrap(bytes);
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
