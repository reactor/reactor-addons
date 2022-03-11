/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.swing;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.swt.widgets.Display;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;

/** 
 * Scheduler that runs tasks on Swt's event dispatch thread. 
 * @deprecated To be removed in 3.6.0. See https://github.com/reactor/reactor-addons/issues/273
 */
@Deprecated
public final class SwtScheduler implements Scheduler {

	/**
	 *
	 * @param display a {@link Display}
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler from(Display display){
		Objects.requireNonNull(display, "display");
		return new SwtScheduler(display);
	}

	final Display display;

	SwtScheduler(Display display) {
		this.display = display;
	}

	@Override
	public Disposable schedule(Runnable task) {
        SwtScheduledDirectAction a = new SwtScheduledDirectAction(task);

        if (!display.isDisposed()) {
            display.asyncExec(a);
        } else {
            throw Exceptions.failWithRejected();
        }

        return a;
	}
	
	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
	    if (delay <= 0) {
	        return schedule(task);
	    }
	    
        if (!display.isDisposed()) {
            SwtScheduledDirectAction a = new SwtScheduledDirectAction(task);

            display.timerExec((int)unit.toMillis(delay), a);
            
            return a;
        }
        throw Exceptions.failWithRejected();
	}
	
	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
	    
        if (!display.isDisposed()) {
    	    long initialDelayMillis = unit.toMillis(initialDelay);
    	    
    	    SwtPeriodicDirectAction a = new SwtPeriodicDirectAction(task, display, 
    	            System.currentTimeMillis() + initialDelayMillis, unit.toMillis(period));
    	    
    	    if (initialDelay <= 0) {
    	        display.asyncExec(a);
    	    } else {
    	        display.timerExec((int)initialDelayMillis, a);
    	    }
	    
    	    return a;
        }
        
        throw Exceptions.failWithRejected();
	}
	
	@Override
	public Worker createWorker() {
		return new SwtWorker(display);
	}

	static final class SwtWorker implements Worker {
		final Display display;

		volatile boolean unsubscribed;

		SwtWorker(Display display) {
			this.display = display;
		}

		@Override
		public void dispose() {
			if (unsubscribed) {
				return;
			}
			unsubscribed = true;
		}

		@Override
		public Disposable schedule(Runnable action) {
	        if (!unsubscribed && !display.isDisposed()) {
	            SwtScheduledAction a = new SwtScheduledAction(action, this);
	            
	            display.asyncExec(a);
	            
	            return a;
	        }
	        
			throw Exceptions.failWithRejected();
		}

		@Override
		public Disposable schedule(Runnable action, long delayTime, TimeUnit unit) {
            if (delayTime <= 0) {
                return schedule(action);
            }

            if (!unsubscribed && !display.isDisposed()) {
                SwtScheduledAction a = new SwtScheduledAction(action, this);

                display.timerExec((int)unit.toMillis(delayTime), a);
                
                return a;
            }
			
            throw Exceptions.failWithRejected();
		}
		
		@Override
		public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
	        if (!display.isDisposed()) {
	            long initialDelayMillis = unit.toMillis(initialDelay);
	            
	            SwtPeriodicAction a = new SwtPeriodicAction(task, this, display, 
	                    System.currentTimeMillis() + initialDelayMillis, unit.toMillis(period));
	            
	            if (initialDelay <= 0) {
	                display.asyncExec(a);
	            } else {
	                display.timerExec((int)initialDelayMillis, a);
	            }
	        
	            return a;
	        }
	        
	        throw Exceptions.failWithRejected();
		}

		/**
		 * Represents a cancellable asynchronous Runnable that wraps an action
		 * and manages the associated Worker lifecycle.
		 */
		static final class SwtScheduledAction 
		extends AtomicBoolean implements Runnable, Disposable {
			/** */
            private static final long serialVersionUID = -2864452628218128444L;

            final Runnable action;

			final SwtWorker parent;

			public SwtScheduledAction(Runnable action, SwtWorker parent) {
				this.action = action;
				this.parent = parent;
			}

			@Override
			public void run() {
				if (!parent.unsubscribed && !get()) {
				    try {
				        action.run();
				    } catch (Throwable ex) {
				        Exceptions.throwIfFatal(ex);
					    Operators.onErrorDropped(ex, Context.empty());
				    }
				}
			}

			@Override
			public void dispose() {
			    set(true);
			}
		}
	}
	
    static final class SwtScheduledDirectAction
    extends AtomicBoolean implements Runnable, Disposable {
        /** */
        private static final long serialVersionUID = 2378266891882031635L;
        
        final Runnable action;

        public SwtScheduledDirectAction(Runnable action) {
            this.action = action;
        }
        
        @Override
        public void run() {
            if (!get()) {
                try {
                    action.run();
                } catch (Throwable ex) {
                    Exceptions.throwIfFatal(ex);
	                Operators.onErrorDropped(ex, Context.empty());
                }
            }
        }
        
        @Override
        public void dispose() {
            set(true);
        }
    }
    
    static final class SwtPeriodicDirectAction extends AtomicBoolean implements Runnable, Disposable {
        /** */
        private static final long serialVersionUID = 1890399765810263705L;

        final Runnable task;
        
        final Display display;
        
        final long periodMillis;
        
        final long start;
        
        long count;

        public SwtPeriodicDirectAction(Runnable task, Display display, long start, long periodMillis) {
            this.task = task;
            this.display = display;
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
                display.asyncExec(this);
            } else {
                display.timerExec((int)delta, this);
            }
        }
        
        @Override
        public void dispose() {
            set(true);
        }
    }

    static final class SwtPeriodicAction extends AtomicBoolean implements Runnable, Disposable {
        /** */
        private static final long serialVersionUID = 1890399765810263705L;

        final Runnable task;
        
        final Display display;
        
        final long periodMillis;
        
        final long start;
        
        final SwtWorker parent;
        
        long count;

        public SwtPeriodicAction(Runnable task, SwtWorker parent, Display display, long start, long periodMillis) {
            this.task = task;
            this.display = display;
            this.start = start;
            this.periodMillis = periodMillis;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            if (get() || parent.unsubscribed) {
                return;
            }
            
            try {
                task.run();
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
	            Operators.onErrorDropped(ex, Context.empty());
                return;
            }

            if (get() || parent.unsubscribed) {
                return;
            }

            long now = System.currentTimeMillis();
            long next = start + (++count) * periodMillis;
            long delta = Math.max(0, next - now);
            
            if (delta == 0) {
                display.asyncExec(this);
            } else {
                display.timerExec((int)delta, this);
            }
        }
        
        @Override
        public void dispose() {
            set(true);
        }
    }

}