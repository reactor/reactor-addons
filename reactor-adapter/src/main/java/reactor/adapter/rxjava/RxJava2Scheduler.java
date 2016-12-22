/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.adapter.rxjava;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.reactivex.disposables.Disposable;
import reactor.core.scheduler.TimedScheduler;

/**
 * Wraps an RxJava scheduler and exposes it as a Reactor-Core Scheduler.
 */
public final class RxJava2Scheduler implements TimedScheduler {

	/**
	 *
	 * @param scheduler an rxjava 2 {@link io.reactivex.Scheduler}
	 * @return a new {@link TimedScheduler}
	 */
	public static TimedScheduler from(io.reactivex.Scheduler scheduler) {
		return new RxJava2Scheduler(scheduler);
	}

	final io.reactivex.Scheduler scheduler;

	RxJava2Scheduler(io.reactivex.Scheduler scheduler) {
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public reactor.core.Disposable schedule(Runnable task) {
		Disposable s = scheduler.scheduleDirect(task);
		return s::dispose;
	}

    @Override
    public reactor.core.Disposable schedule(Runnable task, long delay, TimeUnit unit) {
	    Disposable s = scheduler.scheduleDirect(task, delay, unit);
	    return s::dispose;
    }

    @Override
    public reactor.core.Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
	    Disposable s =
			    scheduler.schedulePeriodicallyDirect(task, initialDelay, period, unit);
	    return s::dispose;
    }

    @Override
    public TimedWorker createWorker() {
        return new RxSchedulerWorker(scheduler.createWorker());
    }
    
    static final class RxSchedulerWorker implements TimedWorker {
        final io.reactivex.Scheduler.Worker w;
        
        public RxSchedulerWorker(io.reactivex.Scheduler.Worker w) {
            this.w = w;
        }

        @Override
        public reactor.core.Disposable schedule(Runnable task) {
            Disposable s = w.schedule(task);
            return s::dispose;
        }

        @Override
        public void shutdown() {
            w.dispose();
        }

        @Override
        public reactor.core.Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            Disposable s = w.schedule(task, delay, unit);
            return s::dispose;
        }

        @Override
        public reactor.core.Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            Disposable s = w.schedulePeriodically(task, initialDelay, period, unit);
            return s::dispose;
        }
    }
}
