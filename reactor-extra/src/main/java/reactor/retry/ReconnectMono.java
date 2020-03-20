package reactor.retry;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.util.context.Context;

public final class ReconnectMono<T> extends Mono<T> implements Invalidate, Disposable, Scannable {

	@SuppressWarnings("rawtypes")
	static final BiConsumer NOOP_ON_VALUE_RECEIVED = (o, o2) -> {};

	@SuppressWarnings("unchecked")
	public static <T> ReconnectMono<T> create(Mono<T> source, Consumer<? super T> onValueExpired) {
		return create(source, onValueExpired, NOOP_ON_VALUE_RECEIVED);
	}

	public static <T> ReconnectMono<T> create(Mono<T> source,
			Consumer<? super T> onValueExpired,
			BiConsumer<? super T, Invalidate> onValueReceived) {
		return new ReconnectMono<>(source, onValueExpired, onValueReceived);
	}


	final Mono<T>                            source;
	final BiConsumer<? super T, Invalidate>  onValueReceived;
	final Consumer<? super T>                onValueExpired;
	final ReconnectMainSubscriber<? super T> mainSubscriber;

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<ReconnectMono> WIP =
		AtomicIntegerFieldUpdater.newUpdater(ReconnectMono.class, "wip");

	volatile ReconnectInner<T>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ReconnectMono, ReconnectInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(ReconnectMono.class, ReconnectInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] EMPTY_UNSUBSCRIBED = new ReconnectInner[0];
	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] EMPTY_SUBSCRIBED   = new ReconnectInner[0];
	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] READY              = new ReconnectInner[0];
	@SuppressWarnings("rawtypes")
	static final ReconnectInner[] TERMINATED         = new ReconnectInner[0];

	static final int ADDED_STATE      = 0;
	static final int READY_STATE      = 1;
	static final int TERMINATED_STATE = 2;

	T         value;
	Throwable t;

	ReconnectMono(
		Mono<T> source,
		Consumer<? super T> onValueExpired,
 		BiConsumer<? super T, Invalidate> onValueReceived
	) {
		this.source = source;
		this.onValueExpired = onValueExpired;
		this.onValueReceived = onValueReceived;
		this.mainSubscriber = new ReconnectMainSubscriber<>(this);

		SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

		final boolean isDisposed = isDisposed();
		if (key == Attr.TERMINATED) return isDisposed;
		if (key == Attr.ERROR) return t;

		return null;
	}

	@Override
	public void dispose() {
		this.terminate(new CancellationException("ReconnectMono has already been disposed"));
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

		final int state = this.add(toAdd);

		if (state == READY_STATE) {
			actual.onNext(this.value);
			actual.onComplete();
		}
		else if (state == TERMINATED_STATE) {
			actual.onError(this.t);
		}
	}

	@SuppressWarnings("unchecked")
	void terminate(Throwable t) {
		if (isDisposed()) {
			return;
		}

		// writes happens before volatile write
		this.t = t;

		final ReconnectInner<T>[] subscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
		if (subscribers == TERMINATED) {
			Operators.onErrorDropped(t, Context.empty());
			return;
		}

		this.mainSubscriber.dispose();

		this.doFinally();

		for (CoreSubscriber<T> consumer : subscribers) {
			consumer.onError(t);
		}
	}

	void complete() {
		ReconnectInner<T>[] subscribers = this.subscribers;
		if (subscribers == TERMINATED) {
			return;
		}

		final T value = this.value;

		for (;;) {
			// ensures TERMINATE is going to be replaced with READY
			if (SUBSCRIBERS.compareAndSet(this, subscribers, READY)) {
				break;
			}

			subscribers = this.subscribers;

			if (subscribers == TERMINATED) {
				this.doFinally();
				return;
			}
		}

		this.onValueReceived.accept(value, this);

		for (ReconnectInner<T> consumer : subscribers) {
			consumer.complete(value);
		}
	}

	void doFinally() {
		if (WIP.getAndIncrement(this) != 0) {
			return;
		}

		int m = 1;
		T value;

		for (;;) {
			value = this.value;

			if (value != null && isDisposed()) {
				this.value = null;
				this.onValueExpired.accept(value);
				return;
			}

			m = WIP.addAndGet(this, -m);
			if (m == 0) {
				return;
			}
		}
	}

	// Check RSocket is not good
	@Override
	public void expire() {
		if (this.subscribers == TERMINATED) {
			return;
		}

		final ReconnectInner<T>[] subscribers = this.subscribers;

		if (subscribers == READY && SUBSCRIBERS.compareAndSet(this, READY, EMPTY_UNSUBSCRIBED)) {
			final T value = this.value;
			this.value = null;

			if (value != null) {
				this.onValueExpired.accept(value);
			}
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
					this.source.subscribe(this.mainSubscriber);
				}
				return ADDED_STATE;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(ReconnectInner<T> ps) {
		for (;;) {
			ReconnectInner<T>[] a = this.subscribers;
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
			}
			else {
				b = new ReconnectInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	final static class ReconnectMainSubscriber<T> implements CoreSubscriber<T> {

		final ReconnectMono<T> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ReconnectMainSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ReconnectMainSubscriber.class, Subscription.class, "s");

		ReconnectMainSubscriber(ReconnectMono<T> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onComplete() {
			final Subscription s = this.s;
			final ReconnectMono<T> p = this.parent;
			final T value = p.value;

			if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
				p.doFinally();
				return;
			}

			if (value == null) {
				p.terminate(new IllegalStateException("Unexpected Completion of the Upstream"));
			}
			else {
				p.complete();
			}
		}

		@Override
		public void onError(Throwable t) {
			final Subscription s = this.s;
			final ReconnectMono<T> p = this.parent;

			if (s == Operators.cancelledSubscription()
					|| S.getAndSet(this, Operators.cancelledSubscription()) == Operators.cancelledSubscription()) {
				p.doFinally();
				Operators.onErrorDropped(t, Context.empty());
				return;
			}

			// terminate upstream which means retryBackoff has exhausted
			p.terminate(t);
		}

		@Override
		public void onNext(T value) {
			if (this.s == Operators.cancelledSubscription()) {
				this.parent.onValueExpired.accept(value);
				return;
			}

			final ReconnectMono<T> p = this.parent;

			p.value = value;
			// volatile write and check on racing
			p.doFinally();
		}

		void dispose() {
			Operators.terminate(S, this);
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
			}
			else {
				this.actual.onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return this.parent;
			return super.scanUnsafe(key);
		}
	}
}