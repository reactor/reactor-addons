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
final class MonoRateLimiterTryAcquire extends Mono<Boolean> {

	final RateLimiterSource rateLimiter;

	MonoRateLimiterTryAcquire(RateLimiterSource rateLimiter) {
		this.rateLimiter = rateLimiter;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Boolean> actual) {
		actual.onSubscribe(new TryAcquireSubscriber(rateLimiter, actual));
	}

	static final class TryAcquireSubscriber implements Subscription, Scannable {

		final RateLimiterSource               rateLimiter;
		final CoreSubscriber<? super Boolean> actual;

		volatile boolean cancelled;

		volatile int once;
		static final AtomicIntegerFieldUpdater<TryAcquireSubscriber> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(TryAcquireSubscriber.class, "once");

		Throwable error;


		public TryAcquireSubscriber(RateLimiterSource rateLimiter,
				CoreSubscriber<? super Boolean> actual) {
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
					actual.onNext(rateLimiter.permitTry());
					actual.onComplete();
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
