/*
 * Copyright (c) 2011-2016 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.scheduler;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import reactor.core.Cancellation;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.Exceptions;

/**
 * Wraps an RxJava scheduler and exposes it as a Reactor-Core Scheduler.
 */
public final class RxScheduler implements TimedScheduler {

    final rx.Scheduler scheduler;
    
    public RxScheduler(rx.Scheduler scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        rx.Scheduler.Worker w = scheduler.createWorker();
        
        rx.Subscription s = w.schedule(() -> {
            try {
                try {
                    task.run();
                } catch (Throwable ex) {
                    Exceptions.throwIfFatal(ex);
                    Exceptions.onErrorDropped(ex);
                }
            } finally {
                w.unsubscribe();
            }
        });
        return s::unsubscribe;
    }

    @Override
    public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
        rx.Scheduler.Worker w = scheduler.createWorker();
        
        rx.Subscription s = w.schedule(() -> {
            try {
                try {
                    task.run();
                } catch (Throwable ex) {
                    Exceptions.throwIfFatal(ex);
                    Exceptions.onErrorDropped(ex);
                }
            } finally {
                w.unsubscribe();
            }
        }, delay, unit);
        return s::unsubscribe;
    }

    @Override
    public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        rx.Scheduler.Worker w = scheduler.createWorker();
        
        rx.Subscription s = w.schedulePeriodically(() -> {
            try {
                task.run();
            } catch (Throwable ex) {
                w.unsubscribe();
                Exceptions.throwIfFatal(ex);
                Exceptions.onErrorDropped(ex);
            }
        }, initialDelay, period, unit);
        return s::unsubscribe;
    }

    @Override
    public TimedWorker createWorker() {
        return new RxSchedulerWorker(scheduler.createWorker());
    }
    
    static final class RxSchedulerWorker implements TimedWorker {
        final rx.Scheduler.Worker w;
        
        public RxSchedulerWorker(rx.Scheduler.Worker w) {
            this.w = w;
        }

        @Override
        public Cancellation schedule(Runnable task) {
            rx.Subscription s = w.schedule(task::run);
            return s::unsubscribe;
        }

        @Override
        public void shutdown() {
            w.unsubscribe();
        }

        @Override
        public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
            rx.Subscription s = w.schedule(task::run, delay, unit);
            return s::unsubscribe;
        }

        @Override
        public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            rx.Subscription s = w.schedulePeriodically(task::run, initialDelay, period, unit);
            return s::unsubscribe;
        }
    }
}
