/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.adapter.rxjava;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import reactor.adapter.rxjava.RxJava3Adapter.DisposableFromRxJava3Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * Wraps an RxJava 3 Scheduler and exposes it as a Reactor-Core {@link Scheduler}.
 */
public final class RxJava3Scheduler implements Scheduler {

	boolean isDisposed = false;

	/**
	 * Adapt an RxJava 3 {@link io.reactivex.rxjava3.core.Scheduler} into a Reactor {@link Scheduler}.
	 *
	 * @param scheduler an RxJava3 {@link io.reactivex.rxjava3.core.Scheduler}
	 *
	 * @return a wrapping {@link Scheduler}
	 */
	public static Scheduler from(io.reactivex.rxjava3.core.Scheduler scheduler) {
		return new RxJava3Scheduler(scheduler);
	}

	final io.reactivex.rxjava3.core.Scheduler scheduler;

	RxJava3Scheduler(io.reactivex.rxjava3.core.Scheduler scheduler) {
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public Worker createWorker() {
		return new RxSchedulerWorker(scheduler.createWorker());
	}

	@Override
	public void dispose() {
		if (!isDisposed) {
			this.isDisposed = true;
			scheduler.shutdown();
		}
	}

	@Override
	public boolean isDisposed() {
		return isDisposed;
	}

	@Override
	public void start() {
		scheduler.start();
	}

	@Override
	public reactor.core.Disposable schedule(Runnable task) {
		return new DisposableFromRxJava3Disposable(scheduler.scheduleDirect(task));
	}

	@Override
	public reactor.core.Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return new DisposableFromRxJava3Disposable(scheduler.scheduleDirect(task,
				delay,
				unit));
	}

	@Override
	public reactor.core.Disposable schedulePeriodically(Runnable task, long initialDelay,
			long period, TimeUnit unit) {
		return new DisposableFromRxJava3Disposable(scheduler.schedulePeriodicallyDirect(
				task,
				initialDelay,
				period,
				unit));
	}

	static final class RxSchedulerWorker implements Worker {

		final io.reactivex.rxjava3.core.Scheduler.Worker w;

		RxSchedulerWorker(io.reactivex.rxjava3.core.Scheduler.Worker w) {
			this.w = w;
		}

		@Override
		public void dispose() {
			w.dispose();
		}

		@Override
		public boolean isDisposed() {
			return w.isDisposed();
		}

		@Override
		public reactor.core.Disposable schedule(Runnable task) {
			return new DisposableFromRxJava3Disposable(w.schedule(task));
		}

		@Override
		public reactor.core.Disposable schedule(Runnable task, long delay,
				TimeUnit unit) {
			return new DisposableFromRxJava3Disposable(w.schedule(task, delay, unit));
		}

		@Override
		public reactor.core.Disposable schedulePeriodically(Runnable task,
				long initialDelay, long period, TimeUnit unit) {
			return new DisposableFromRxJava3Disposable(w.schedulePeriodically(task, initialDelay, period, unit));
		}
	}
}
