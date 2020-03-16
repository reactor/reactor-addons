package reactor.retry;

import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import static reactor.retry.AbstractRetry.RETRY_EXHAUSTED;
import static reactor.retry.DefaultReconnect.RETRY_PREDICATE;

final class ReconnectMono<T> extends Mono<T>
		implements CoreSubscriber<T>, Runnable,
		           Disposable {

	final DefaultReconnect<T, ?> reconnectConfig;
	final BiConsumer<? super T, Runnable> resetHook;
	final Consumer<? super T> onDisposeHook;
	final Mono<T> source;
	final DefaultContext<?> context;
	final Scheduler scheduler;

	volatile ReconnectInner<T>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ReconnectMono, ReconnectInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(
					ReconnectMono.class, ReconnectInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] EMPTY_UNSUBSCRIBED = new ReconnectInner[0];
	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] EMPTY_SUBSCRIBED = new ReconnectInner[0];
	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] READY = new ReconnectInner[0];
	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] TERMINATED = new ReconnectInner[0];

	static final int ADDED_STATE = 0;
	static final int READY_STATE = 1;
	static final int TERMINATED_STATE = 2;


	T value;
	Throwable t;
	long iteration = -1;
	Disposable disposable;

	Instant timeout;
	Instant resetTime;

	ReconnectMono(
			DefaultReconnect<T, ?> reconnectConfig,
			BiConsumer<? super T, Runnable> resetHook) {
		this.reconnectConfig = reconnectConfig;
		this.source = reconnectConfig.source;
		this.context = new DefaultContext<>(reconnectConfig.applicationContext, 0L, null, null);
		this.scheduler = reconnectConfig.backoffScheduler == null
				? Schedulers.parallel()
				: reconnectConfig.backoffScheduler;
		this.resetHook = resetHook;
		this.onDisposeHook = reconnectConfig.onDispose;

		SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
	}

	@Override
	public void dispose() {
		terminate(new CancellationException("ReconnectMono has already been disposed"));
	}

	@Override
	public boolean isDisposed() {
		return this.subscribers == TERMINATED;
	}

	@Override
	@SuppressWarnings("uncheked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		final ReconnectInner<T> toAdd = new ReconnectInner<>(actual, this);
		actual.onSubscribe(toAdd);

		final int state = add(toAdd);

		if (state == READY_STATE) {
			actual.onNext(this.value);
			actual.onComplete();
		} else if (state == TERMINATED_STATE) {
			final Throwable t = this.t;
			// can be null since value assigment happens after the volatile set
			if (t == null) {
				actual.onError(new CancellationException("ReconnectMono has already been disposed"));
			} else {
				actual.onError(t);
			}
		}
	}

	void terminate(Throwable t) {
		@SuppressWarnings("unchecked")
		final CoreSubscriber<T>[] consumers = SUBSCRIBERS.getAndSet(this, TERMINATED);
		if (consumers == TERMINATED) {
			return;
		}

		final T value = this.value;
		final Disposable disposable = this.disposable;

		this.t = t;
		this.value = null;
		this.disposable = null;

		if (disposable != null && !disposable.isDisposed()) {
			disposable.dispose();
		}

		if (value != null) {
			this.onDisposeHook.accept(value);
		}

		for (CoreSubscriber<T> consumer : consumers) {
			consumer.onError(t);
		}
	}

	// Check RSocket is not good
	void reset() {
		if (this.subscribers == TERMINATED) {
			return;
		}

		final ReconnectInner<T>[] subscribers = this.subscribers;

		if (subscribers == READY && SUBSCRIBERS.compareAndSet(this, READY, EMPTY_UNSUBSCRIBED)) {
			this.resetTime = Instant.now(this.reconnectConfig.clock);
			this.value = null;
		}
	}

	int add(ReconnectInner<T> ps) {
		for (;;) {
			ReconnectInner<T>[] a = this.subscribers;

			if (a == TERMINATED) {
				return TERMINATED_STATE;
			}

			if (a == READY) {
				return READY_STATE;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			ReconnectInner<T>[] b = new ReconnectInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				if (a == EMPTY_UNSUBSCRIBED) {
					reconnect(null);
				}
				return ADDED_STATE;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(ReconnectInner<T> ps) {
		for (;;) {
			ReconnectInner<T>[] a = subscribers;
			int n = a.length;
			if (n == 0) {
				return;
			}

			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			ReconnectInner<T>[] b;

			if (n == 1) {
				b = EMPTY_SUBSCRIBED;
			} else {
				b = new ReconnectInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void run() {
		this.source.subscribe(this);
	}

	@Override
	public void onSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE);
	}

	@Override
	public void onComplete() {
		final T value = this.value;

		if (value == null) {
			reconnect(null);
		} else {
			// happens-before write so all non-volatile writes are going to be available on this volatile field read
			this.iteration = 0;
			@SuppressWarnings("unchecked")
			final ReconnectInner<T>[] consumers = SUBSCRIBERS.getAndSet(this, READY);

			if (consumers == TERMINATED) {
				this.dispose();
				return;
			}

			this.resetHook.accept(value, this::reset);

			for (ReconnectInner<T> consumer : consumers) {
				consumer.complete(value);
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		this.value = null;
		reconnect(t);
	}

	@Override
	public void onNext(@Nullable T value) {
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	void reconnect(Throwable t) {
		final long iteration = this.iteration;
		if (iteration == -1) {
			this.iteration = 1;
			this.timeout = this.reconnectConfig.calculateTimeout();
			this.resetTime = Instant.MAX;
			this.run();
		} else {
			if (iteration == 0) {
				this.timeout = this.reconnectConfig.calculateTimeout();
			}
			final BackoffDelay delay = this.reconnectConfig.reconnectBackoff(iteration,
				this.resetTime,
				this.timeout,
				(DefaultContext<T>) this.context,
				t
			);

			if (delay == RETRY_EXHAUSTED) {
				this.terminate(new RetryExhaustedException(t));
				return;
			} else if (delay == RETRY_PREDICATE) {
				this.terminate(t);
				return;
			}

			this.iteration = iteration + 1;
			this.disposable = this.scheduler.schedule(this,
					delay.delay()
					     .toNanos(),
					TimeUnit.NANOSECONDS);
		}
	}

	final static class ReconnectInner<T> extends MonoSubscriber<T, T> {
		final ReconnectMono<T> parent;

		ReconnectInner(CoreSubscriber<? super T> actual, ReconnectMono<T> parent) {
			super(actual);
			this.parent = parent;
		}

		@Override
		public void cancel() {
			if(!isCancelled()) {
				super.cancel();
				this.parent.remove(this);
			}
		}

		@Override
		public void onComplete() {
			if (!isCancelled()) {
				this.actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (isCancelled()) {
				Operators.onErrorDropped(t, currentContext());
			} else {
				this.actual.onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return this.parent;
			}
			return super.scanUnsafe(key);
		}
	}
}