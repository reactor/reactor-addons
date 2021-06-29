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

/**
 * Replace the standard schedulers of RxJava 2 or Reactor-Core with
 * the schedulers of the other library.
 * <p>
 * This can prevent having two sets of worker pools when
 * both libraries are in use.
 */
public final class RxJava2Schedulers {

    /** Utility class. */
    private RxJava2Schedulers() {
        throw new IllegalStateException("No instances!");
    }
    
    /**
     * The RxJava 2 standard Schedulers will be replaced
     * by their Reactor-Core counterparts; both libraries
     * will run on the Reactor-Core schedulers.
     * <p>
     * Call this before interacting with RxJava 2.
     */
    public static void useReactorCoreSchedulers() {
        // shut down the standard schedulers
        if (io.reactivex.plugins.RxJavaPlugins.getComputationSchedulerHandler() == null) {
            io.reactivex.schedulers.Schedulers.computation().shutdown();
        }
        if (io.reactivex.plugins.RxJavaPlugins.getIoSchedulerHandler() == null) {
            io.reactivex.schedulers.Schedulers.io().shutdown();
        }
        if (io.reactivex.plugins.RxJavaPlugins.getSingleSchedulerHandler() == null) {
            io.reactivex.schedulers.Schedulers.single().shutdown();
        }
        if (io.reactivex.plugins.RxJavaPlugins.getNewThreadSchedulerHandler() == null) {
            io.reactivex.schedulers.Schedulers.newThread().shutdown();
        }
        
        io.reactivex.Scheduler computation = ReactorCoreSchedulerWrapper.from(reactor.core.scheduler.Schedulers.parallel());
        io.reactivex.plugins.RxJavaPlugins.setComputationSchedulerHandler(s -> computation);
        
        io.reactivex.Scheduler elastic = ReactorCoreSchedulerWrapper.from(reactor.core.scheduler.Schedulers.elastic());
        io.reactivex.plugins.RxJavaPlugins.setIoSchedulerHandler(s -> elastic);
        
        io.reactivex.Scheduler single = ReactorCoreSchedulerWrapper.from(reactor.core.scheduler.Schedulers.single());
        io.reactivex.plugins.RxJavaPlugins.setSingleSchedulerHandler(s -> single);

        // There is no scheduler equivalent to RxJava's newThread so let's use elastic
        io.reactivex.plugins.RxJavaPlugins.setNewThreadSchedulerHandler(s -> elastic);
    }
    
    /**
     * Restores the RxJava 2 standard Schedulers to their
     * default values.
     */
    public static void resetReactorCoreSchedulers() {
        io.reactivex.plugins.RxJavaPlugins.setComputationSchedulerHandler(null);
        
        io.reactivex.plugins.RxJavaPlugins.setIoSchedulerHandler(null);
        
        io.reactivex.plugins.RxJavaPlugins.setSingleSchedulerHandler(null);

        io.reactivex.plugins.RxJavaPlugins.setNewThreadSchedulerHandler(null);

        // restart the standard schedulers
        io.reactivex.schedulers.Schedulers.computation().start();
        io.reactivex.schedulers.Schedulers.io().start();
        io.reactivex.schedulers.Schedulers.single().start();
        io.reactivex.schedulers.Schedulers.newThread().start();
    }

    /**
     * Wraps a Reactor-Core Scheduler and exposes it as an RxJava 2 Scheduler.
     */
    static final class ReactorCoreSchedulerWrapper extends io.reactivex.Scheduler {

        /**
         * Wraps a Reactor-Core Scheduler and exposes it as an RxJava 2 Scheduler.
         * @param scheduler an Reactor-Core {@link reactor.core.scheduler.Scheduler}
         * @return a new {@link io.reactivex.Scheduler} instance
         */
        public static io.reactivex.Scheduler from(reactor.core.scheduler.Scheduler scheduler) {
            return new ReactorCoreSchedulerWrapper(scheduler);
        }

        final reactor.core.scheduler.Scheduler scheduler;

        ReactorCoreSchedulerWrapper(reactor.core.scheduler.Scheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        }

        @Override
        public io.reactivex.disposables.Disposable scheduleDirect(Runnable task) {
            reactor.core.Disposable s = scheduler.schedule(task);
            return io.reactivex.disposables.Disposables.fromAction(s::dispose);
        }

        @Override
        public io.reactivex.disposables.Disposable scheduleDirect(Runnable task, long delay, TimeUnit unit) {
            reactor.core.Disposable s = scheduler.schedule(task, delay, unit);
            return io.reactivex.disposables.Disposables.fromAction(s::dispose);
        }

        @Override
        public io.reactivex.disposables.Disposable schedulePeriodicallyDirect(Runnable task, long initialDelay, long period, TimeUnit unit) {
            reactor.core.Disposable s =
                    scheduler.schedulePeriodically(task, initialDelay, period, unit);
            return io.reactivex.disposables.Disposables.fromAction(s::dispose);
        }

        @Override
        public Worker createWorker() {
            return new ReactorCoreSchedulerWorker(scheduler.createWorker());
        }
        
        /**
         * An RxJava 2 Worker that wraps a Reactor Core Worker.
         */
        static final class ReactorCoreSchedulerWorker extends io.reactivex.Scheduler.Worker {
            final reactor.core.scheduler.Scheduler.Worker w;

            volatile boolean disposed;
            
            ReactorCoreSchedulerWorker(reactor.core.scheduler.Scheduler.Worker w) {
                this.w = w;
            }

            @Override
            public io.reactivex.disposables.Disposable schedule(Runnable task) {
                reactor.core.Disposable s = w.schedule(task);
                return io.reactivex.disposables.Disposables.fromAction(s::dispose);
            }

            @Override
            public void dispose() {
                disposed = true;
                w.dispose();
            }

            @Override
            public boolean isDisposed() {
                return disposed;
            }
            
            @Override
            public io.reactivex.disposables.Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                reactor.core.Disposable s = w.schedule(task, delay, unit);
                return io.reactivex.disposables.Disposables.fromAction(s::dispose);
            }

            @Override
            public io.reactivex.disposables.Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
                reactor.core.Disposable s = w.schedulePeriodically(task, initialDelay, period, unit);
                return io.reactivex.disposables.Disposables.fromAction(s::dispose);
            }
        }
    }
}
