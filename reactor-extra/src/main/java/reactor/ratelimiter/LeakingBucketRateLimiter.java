package reactor.ratelimiter;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

/**
 * @author Simon Baslé
 */
final class LeakingBucketRateLimiter implements RateLimiter,
                                                RateLimiterSource,
                                                Runnable {

	final Duration  permitRefreshPeriod;
	final int       permitRefreshCount;
	final Scheduler scheduler;

	final Disposable.Composite permitListeners;
	final Queue<PermitConsumer> permitQueue;

	volatile Disposable replenishTask;
	static final AtomicReferenceFieldUpdater<LeakingBucketRateLimiter, Disposable> REPLENISH_TASK =
			AtomicReferenceFieldUpdater.newUpdater(LeakingBucketRateLimiter.class, Disposable.class, "replenishTask");

	volatile int permits;
	static final AtomicIntegerFieldUpdater<LeakingBucketRateLimiter> PERMITS =
			AtomicIntegerFieldUpdater.newUpdater(LeakingBucketRateLimiter.class, "permits");

	LeakingBucketRateLimiter(Duration permitRefreshPeriod,
			int permitRefreshCount,
			Scheduler scheduler) {
		this.permitRefreshPeriod = permitRefreshPeriod;
		this.permitRefreshCount = permitRefreshCount;
		this.scheduler = scheduler;
		this.permitListeners = Disposables.composite();
		this.permitQueue = Queues.<PermitConsumer>unboundedMultiproducer().get();
		PERMITS.compareAndSet(this, 0, permitRefreshCount);
	}

	@Override
	public void run() {
		permits = permitRefreshCount;
		for(;;) {
			PermitConsumer nextInLine = permitQueue.peek();
			if (nextInLine == null) {
				break;
			}

			if (nextInLine.isDisposed()) {
				permitQueue.poll();
				continue; //permitconsumer was cancelled while waiting
			}

			if (!permitTry()) {
				break;
			}
			else {
				permitQueue.poll().permitAcquired();
			}
		}
	}

	void scheduleReplenishIfNecessary() {
		if (replenishTask == null) {
			Disposable rt = scheduler.schedulePeriodically(this,
					permitRefreshPeriod.toMillis(),
					permitRefreshPeriod.toMillis(),
					TimeUnit.MILLISECONDS);
			if (!REPLENISH_TASK.compareAndSet(this, null, rt)) {
				rt.dispose();
			}
		}
	}

	@Override
	public Mono<Void> acquireOrReject() {
		scheduleReplenishIfNecessary();
		return new MonoRateLimiterAcquireOrReject(this);
	}

	@Override
	public Mono<Void> acquireOrWait() {
		scheduleReplenishIfNecessary();
		return new MonoRateLimiterAcquire(this);
	}

	@Override
	public Mono<Boolean> tryAcquire() {
		scheduleReplenishIfNecessary();
		return new MonoRateLimiterTryAcquire(this);
	}

	@Override
	public boolean permitTry() {
		if (permitListeners.isDisposed()) {
			throw new IllegalStateException("The RateLimiter has been disposed");
		}
		for(;;) {
			int p = permits;
			if (p <= 0) {
				return false;
			}
			if (PERMITS.compareAndSet(this, p, p-1)) {
				return true;
			}
		}
	}

	@Override
	public void waitForPermit(PermitConsumer permitConsumer) {
		if (permitListeners.add(permitConsumer)) {
			if (permitTry()) {
				//short-circuit, a permit is already available
				permitListeners.remove(permitConsumer);
				scheduler.schedule(permitConsumer::permitAcquired);
			}
			else {
				permitQueue.add(permitConsumer);
			}
		}
		else {
			permitConsumer.permitRejected(new IllegalStateException("The RateLimiter has been disposed"));
		}
	}

	@Override
	public void cancelForPermit(PermitConsumer permitConsumer) {
		if (permitListeners.remove(permitConsumer)) {
			//permitConsumer is already marked as disposed, will at least be ignored during replenishment
			//TODO how to remove from MPSC Queue? avoid retaining reference? cleanup as you go?
		}
	}

	@Override
	public int permits() {
		return permits;
	}

	@Override
	public boolean isDisposed() {
		return permitListeners.isDisposed();
	}

	@Override
	public void dispose() {
		permitQueue.clear();
		permitListeners.dispose();
		//also disposes the periodic task
		Disposable rt = REPLENISH_TASK.getAndSet(this, null);
		if (rt != null) {
			rt.dispose();
		}
	}
}
