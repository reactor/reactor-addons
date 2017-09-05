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

package reactor.adapter.akka;

import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

/**
 * A Scheduler implementation that given an ActorSystem, creates a single Actor and
 * routes all Scheduler/Worker calls to it.
 */
public class ActorScheduler implements reactor.core.scheduler.Scheduler {

	/**
	 *
	 * @param system an {@link ActorSystem}
	 * @return a new {@link reactor.core.scheduler.Scheduler}
	 */
	public static reactor.core.scheduler.Scheduler from(ActorSystem system) {
		Objects.requireNonNull(system, "system");
		return new ActorScheduler(system.actorOf(Props.create(ActorExecutor.class)));
	}

	/**
	 *
	 * @param actorRef an {@link ActorRef}
	 * @return a new {@link reactor.core.scheduler.Scheduler}
	 */
	public static reactor.core.scheduler.Scheduler from(ActorRef actorRef) {
		Objects.requireNonNull(actorRef, "actorRef");
		return new ActorScheduler(actorRef);
	}

    final ActorRef actor;

	ActorScheduler(ActorRef actor) {
		this.actor = actor;
	}

	@Override
    public Disposable schedule(Runnable task) {
        DirectRunnable dr = new DirectRunnable(task);
        actor.tell(dr, ActorRef.noSender());
        return dr;
    }

    @Override
    public Worker createWorker() {
        return new ActorWorker(actor);
    }

    
    static final class ActorWorker implements Worker {

        final ActorRef actor;
        
        HashSet<WorkerRunnable> tasks;
        
        public ActorWorker(ActorRef actor) {
            this.actor = actor;
            this.tasks = new HashSet<>();
        }
        
        @Override
        public Disposable schedule(Runnable task) {
            WorkerRunnable wr = new WorkerRunnable(task, this);
            
            synchronized (this) {
                HashSet<WorkerRunnable> set = tasks;
                if (set == null) {
                    throw Exceptions.failWithRejected();
                }
                set.add(wr);
            }
            
            actor.tell(wr, ActorRef.noSender());
            
            return wr;
        }

        @Override
        public void dispose() {
            HashSet<WorkerRunnable> set;
            
            synchronized (this) {
                set = tasks;
                tasks = null;
            }
            
            if (set != null) {
                for (WorkerRunnable wr : set) {
                    wr.delete();
                }
            }
        }
        
        void delete(WorkerRunnable run) {
            synchronized (this) {
                HashSet<WorkerRunnable> set = tasks;
                if (set == null) {
                    return;
                }
                set.remove(run);
            }
        }
    }

    static final class DirectRunnable 
    extends AtomicBoolean implements Runnable, Disposable {
        /** */
        private static final long serialVersionUID = -8208677295345126172L;
        
        final Runnable run;
        
        public DirectRunnable(Runnable run) {
            this.run = run;
        }
        
        @Override
        public void run() {
            if (!get()) {
                run.run();
            }
        }
        
        @Override
        public void dispose() {
            set(true);
        }
    }
    
    static final class WorkerRunnable 
    extends AtomicBoolean implements Runnable, Disposable {
        /** */
        private static final long serialVersionUID = -1760219254778525714L;

        final Runnable run;
        
        final ActorWorker parent;

        public WorkerRunnable(Runnable run, ActorWorker parent) {
            this.run = run;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            if (!get()) {
                try {
                    run.run();
                } finally {
                    if (compareAndSet(false, true)) {
                        parent.delete(this);
                    }
                }
            }
        }
        
        @Override
        public void dispose() {
            if (compareAndSet(false, true)) {
                parent.delete(this);
            }
        }
        
        public void delete() {
            set(true);
        }
    }

    static final class ActorExecutor extends UntypedActor {

        @Override
        public void onReceive(Object message) throws Exception {
            Runnable r = (Runnable)message;
            
            try {
                r.run();
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                Operators.onErrorDropped(ex, Context.empty());
            }
        }
    }
}
