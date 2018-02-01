package reactor.ratelimiter;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

/**
 * @author Simon Baslé
 */
final class MonoRateLimiterAcquireOrReject extends Mono<Void> {

	final RateLimiterSource rateLimiter;

	MonoRateLimiterAcquireOrReject(RateLimiterSource rateLimiter) {
		this.rateLimiter = rateLimiter;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Void> actual) {
		actual.onSubscribe(new AcquireOrRejectSubscriber(rateLimiter, actual));
	}

	static final class AcquireOrRejectSubscriber implements Subscription, Scannable {

		final RateLimiterSource            rateLimiter;
		final CoreSubscriber<? super Void> actual;

		volatile boolean cancelled;

		volatile int once;
		static final AtomicIntegerFieldUpdater<AcquireOrRejectSubscriber> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(AcquireOrRejectSubscriber.class, "once");

		Throwable error;


		public AcquireOrRejectSubscriber(RateLimiterSource rateLimiter,
				CoreSubscriber<? super Void> actual) {
			this.rateLimiter = rateLimiter;
			this.actual = actual;
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l) && ONCE.compareAndSet(this, 0, 1)) {
				if (cancelled) {
					return;
				}
				try {
					if (rateLimiter.permitTry()) {
						actual.onComplete();
					}
					else {
						error = new RateLimitedException();
						actual.onError(error);
					}
				}
				catch (Throwable ex) {
					error = ex;
					actual.onError(ex);
				}
			}
		}

		@Override
		public void cancel() {
			ONCE.compareAndSet(this, 0, 2);
		}

		@Override
		public Object scanUnsafe(Attr attr) {
			if (attr == Attr.CANCELLED) return once == 2;
			if (attr == Attr.TERMINATED) return once == 1;
			if (attr == Attr.ERROR) return error;

			return null;
		}
	}
}
