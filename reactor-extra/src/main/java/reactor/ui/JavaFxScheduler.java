/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.ui;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javafx.application.Platform;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;

/**
 * Scheduler that runs tasks on JavaFx's event dispatch thread.
 * <p>
 * This class was adapted from the
 * io.reactivex.rxjavafx.schedulers.JavaFxScheduler class from
 * https://github.com/ReactiveX/RxJavaFX
 * 
 * <ul>
 * <li> Make use of Reactor types instead of RxJava types
 * <li> Make use of a java.util.Timer instead of javafx.animation.TimeLine
 * </ul>
 * </p>
 * 
 * @see io.reactivex.rxjavafx.schedulers.JavaFxScheduler
 */
public final class JavaFxScheduler implements Scheduler {

	private static final JavaFxScheduler INSTANCE = new JavaFxScheduler();

	public static JavaFxScheduler platform() {
		return INSTANCE;
	}

	private static void assertThatTheDelayIsValidForTheJavaFxTimer(long delay) {
		if (delay < 0 || delay > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(String.format(
					"The JavaFx timer only accepts non-negative delays up to %d milliseconds.", Integer.MAX_VALUE));
		}
	}

	JavaFxScheduler() {
	}

	@Override
	public Disposable schedule(Runnable task) {
		JavaFxWorker javaFxWorker = new JavaFxWorker();
		return javaFxWorker.schedule(task);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		JavaFxWorker javaFxWorker = new JavaFxWorker();
		return javaFxWorker.schedule(task, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		JavaFxWorker javaFxWorker = new JavaFxWorker();
		long initialDelayMillis = unit.toMillis(initialDelay);

		FxPeriodicDirectAction a = new FxPeriodicDirectAction(task, javaFxWorker,
				System.currentTimeMillis() + initialDelayMillis, unit.toMillis(period));

		if (initialDelay <= 0) {
			javaFxWorker.schedule(a);
		} else {
			javaFxWorker.schedule(a, initialDelayMillis, TimeUnit.MILLISECONDS);
		}

		return a;
	}

	@Override
	public Worker createWorker() {
		return new JavaFxWorker();
	}

	/**
	 * A Worker implementation which manages a queue of QueuedRunnable for execution
	 * on the Java FX Application thread For a simpler implementation the queue
	 * always contains at least one element. {@link #head} is the element, which is
	 * in execution or was last executed {@link #tail} is an atomic reference to the
	 * last element in the queue, or null when the worker was disposed Recursive
	 * actions are not preferred and inserted at the tail of the queue as any other
	 * action would be The Worker will only schedule a single job with
	 * {@link Platform#runLater(Runnable)} for when the queue was previously empty
	 */
	private static class JavaFxWorker implements Worker, Runnable {
		private volatile QueuedRunnable head = new QueuedRunnable(null); /// only advanced in run(), initialised with a
																			/// starter element
		private final AtomicReference<QueuedRunnable> tail = new AtomicReference<>(head); /// points to the last
																							/// element, null when
																							/// disposed

		private static class QueuedRunnable extends AtomicReference<QueuedRunnable> implements Disposable, Runnable {
			private volatile Runnable action;

			private QueuedRunnable(Runnable action) {
				this.action = action;
			}

			@Override
			public void dispose() {
				action = null;
			}

			@Override
			public boolean isDisposed() {
				return action == null;
			}

			@Override
			public void run() {
				Runnable action = this.action;
				if (action != null) {
					action.run();
				}
				this.action = null;
			}
		}

		@Override
		public void dispose() {
			tail.set(null);
			QueuedRunnable qr = this.head;
			while (qr != null) {
				qr.dispose();
				qr = qr.getAndSet(null);
			}
		}

		@Override
		public boolean isDisposed() {
			return tail.get() == null;
		}

		@Override
		public Disposable schedule(final Runnable action, long delayTime, TimeUnit unit) {
			long delay = Math.max(0, unit.toMillis(delayTime));
			assertThatTheDelayIsValidForTheJavaFxTimer(delay);

			final QueuedRunnable queuedRunnable = new QueuedRunnable(action);
			if (delay == 0) { // delay is too small for the java fx timer, schedule it without delay
				return schedule(queuedRunnable);
			}

			Timer timer = new Timer();
			timer.schedule(new TimerTask() {

				@Override
				public void run() {
					queuedRunnable.run();
				}
			}, delay);

			return () -> {
				queuedRunnable.dispose();
				timer.cancel();
			};
		}

		@Override
		public Disposable schedule(final Runnable action) {
			if (isDisposed()) {
				return Disposables.disposed();
			}

			final QueuedRunnable queuedRunnable = action instanceof QueuedRunnable ? (QueuedRunnable) action
					: new QueuedRunnable(action);

			QueuedRunnable tailPivot;
			do {
				tailPivot = tail.get();
			} while (tailPivot != null && !tailPivot.compareAndSet(null, queuedRunnable));

			if (tailPivot == null) {
				queuedRunnable.dispose();
			} else {
				tail.compareAndSet(tailPivot, queuedRunnable); // can only fail with a concurrent dispose and we don't
																// want to override the disposed value
				if (tailPivot == head) {
					if (Platform.isFxApplicationThread()) {
						run();
					} else {
						Platform.runLater(this);
					}
				}
			}
			return queuedRunnable;
		}

		@Override
		public void run() {
			for (QueuedRunnable qr = head.get(); qr != null; qr = qr.get()) {
				qr.run();
				head = qr;
			}
		}
	}

	static final class FxPeriodicDirectAction extends AtomicBoolean implements Runnable, Disposable {
		/** */
		private static final long serialVersionUID = 1890399765810263705L;

		final Runnable task;

		final long periodMillis;

		final long start;

		long count;

		private JavaFxWorker javaFxWorker;

		public FxPeriodicDirectAction(Runnable task, JavaFxWorker javaFxWorker, long start, long periodMillis) {
			this.task = task;
			this.javaFxWorker = javaFxWorker;
			this.start = start;
			this.periodMillis = periodMillis;
		}

		@Override
		public void run() {
			if (get()) {
				return;
			}

			try {
				task.run();
			} catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				Operators.onErrorDropped(ex, Context.empty());
				return;
			}

			if (get()) {
				return;
			}

			long now = System.currentTimeMillis();
			long next = start + (++count) * periodMillis;
			long delta = Math.max(0, next - now);

			if (delta == 0) {
				javaFxWorker.schedule(this);
			} else {
				javaFxWorker.schedule(this, delta, TimeUnit.MILLISECONDS);
			}
		}

		@Override
		public void dispose() {
			set(true);
		}
	}
}
