package reactor.ratelimiter;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

/**
 * @author Simon Baslé
 */
final class MonoRateLimiterAcquire extends Mono<Void> {

	final RateLimiterSource rateLimiter;

	MonoRateLimiterAcquire(RateLimiterSource rateLimiter) {
		this.rateLimiter = rateLimiter;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Void> actual) {
		actual.onSubscribe(new AcquireSubscriber(rateLimiter, actual));
	}

	static final class AcquireSubscriber implements PermitConsumer, Subscription, Scannable {

		final RateLimiterSource rateLimiter;
		final CoreSubscriber<? super Void> actual;

		private static final int STATE_NO_REQUEST = 0;
		private static final int STATE_REQUESTED = 1;
		private static final int STATE_CANCELLED = 2;
		private static final int STATE_DONE = 3;


		volatile int state;
		static final AtomicIntegerFieldUpdater<AcquireSubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(AcquireSubscriber.class, "state");

		Throwable error;

		public AcquireSubscriber(RateLimiterSource rateLimiter,
				CoreSubscriber<? super Void> actual) {
			this.rateLimiter = rateLimiter;
			this.actual = actual;
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l) && STATE.compareAndSet(this, STATE_NO_REQUEST, STATE_REQUESTED)) {
				rateLimiter.waitForPermit(this);
			}
		}

		@Override
		public void cancel() {
			if (!STATE.compareAndSet(this, STATE_NO_REQUEST, STATE_CANCELLED)) {
				if (STATE.compareAndSet(this, STATE_REQUESTED, STATE_CANCELLED)) {
					rateLimiter.cancelForPermit(this);
				}
			}
		}

		@Override
		public void permitAcquired() {
			actual.onComplete();
		}

		@Override
		public void permitRejected(Throwable cause) {
			actual.onError(cause);
		}

		@Override
		public boolean isDisposed() {
			return state == STATE_CANCELLED;
		}

		@Override
		public void dispose() {
			cancel();
		}

		@Override
		public Object scanUnsafe(Attr attr) {
			if (attr == Attr.CANCELLED) return state == STATE_CANCELLED;
			if (attr == Attr.TERMINATED) return state == STATE_DONE;
			if (attr == Attr.ERROR) return error;

			return null;
		}
	}
}
