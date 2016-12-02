package reactor.test.publisher;

import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Operators;

/**
 * @author Simon Baslé
 */
public class AssertPublisher<T> implements Publisher<T> {

	public static <T> AssertPublisher<T> createMisbehaving(Misbehavior first, Misbehavior... rest) {
		return new AssertPublisher<T>(first, rest);
	}

	public static <T> AssertPublisher<T> create() {
		return new AssertPublisher<T>();
	}

	public enum Misbehavior {
		REQUEST_OVERFLOW, ALLOW_NULL;
	}

	@SuppressWarnings("rawtypes")
	private static final AssertPublisherSubcription[] EMPTY = new AssertPublisherSubcription[0];

	@SuppressWarnings("rawtypes")
	private static final AssertPublisherSubcription[] TERMINATED = new AssertPublisherSubcription[0];

	volatile int cancelCount;

	static final AtomicIntegerFieldUpdater<AssertPublisher> CANCEL_COUNT =
		AtomicIntegerFieldUpdater.newUpdater(AssertPublisher.class, "cancelCount");

	Throwable error;

	volatile boolean hasOverflown;

	final EnumSet<Misbehavior> misbehaviors;

	@SuppressWarnings("unchecked")
	volatile AssertPublisherSubcription<T>[] subscribers = EMPTY;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<AssertPublisher, AssertPublisherSubcription[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(AssertPublisher.class, AssertPublisherSubcription[].class, "subscribers");

	AssertPublisher(Misbehavior first, Misbehavior... rest) {
		this.misbehaviors = EnumSet.of(first, rest);
	}

	AssertPublisher() {
		this.misbehaviors = EnumSet.noneOf(Misbehavior.class);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Objects.requireNonNull(s, "s");

		AssertPublisherSubcription<T>
				p = new AssertPublisherSubcription<>(s, this);
		s.onSubscribe(p);

		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
		} else {
			Throwable e = error;
			if (e != null) {
				s.onError(e);
			} else {
				s.onComplete();
			}
		}
	}

	boolean add(AssertPublisherSubcription<T> s) {
		AssertPublisherSubcription<T>[] a = subscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked") AssertPublisherSubcription<T>[] b = new AssertPublisherSubcription[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(AssertPublisherSubcription<T> s) {
		AssertPublisherSubcription<T>[] a = subscribers;
		if (a == TERMINATED || a == EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int len = a.length;

			int j = -1;

			for (int i = 0; i < len; i++) {
				if (a[i] == s) {
					j = i;
					break;
				}
			}
			if (j < 0) {
				return;
			}
			if (len == 1) {
				subscribers = EMPTY;
				return;
			}

			AssertPublisherSubcription<T>[] b = new AssertPublisherSubcription[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	public long downstreamCount() {
		return subscribers.length;
	}

	public boolean hasDownstreams() {
		AssertPublisherSubcription<T>[] s = subscribers;
		return s != EMPTY && s != TERMINATED;
	}

	static final class AssertPublisherSubcription<T> implements Subscription {

		final Subscriber<? super T> actual;

		final AssertPublisher<T> parent;

		volatile boolean cancelled;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AssertPublisherSubcription>
				REQUESTED =
				AtomicLongFieldUpdater.newUpdater(AssertPublisherSubcription.class, "requested");

		public AssertPublisherSubcription(Subscriber<? super T> actual, AssertPublisher<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				AssertPublisher.CANCEL_COUNT.incrementAndGet(parent);
				parent.remove(this);
			}
		}

		void onNext(T value) {
			long r = requested;
			if (r != 0L || parent.misbehaviors.contains(Misbehavior.REQUEST_OVERFLOW)) {
				if (r == 0) {
					parent.hasOverflown = true;
				}
				actual.onNext(value);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return;
			}
			parent.remove(this);
			actual.onError(new IllegalStateException("Can't deliver value due to lack of requests"));
		}

		void onError(Throwable e) {
			actual.onError(e);
		}

		void onComplete() {
			actual.onComplete();
		}
	}


	//TODO should we offer to assert total request? how to do it?

	/**
	 * Asserts that this publisher has subscribers.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertSubscribers() {
		AssertPublisherSubcription<T>[] s = subscribers;
		if (s == EMPTY || s == TERMINATED) {
			throw new AssertionError("Expected subscribers");
		}
		return this;
	}

	/**
	 * Asserts that this publisher has exactly n subscribers.
	 *
	 * @param n the expected number of subscribers
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertSubscribers(int n) {
		int sl = subscribers.length;
		if (sl != n) {
			throw new AssertionError("Expected " + n + " subscribers, got " + sl);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has no subscribers.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertNoSubscribers() {
		int sl = subscribers.length;
		if (sl != 0) {
			throw new AssertionError("Expected no subscribers, got " + sl);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had at least one subscriber that has been cancelled.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertCancelled() {
		if (cancelCount == 0) {
			throw new AssertionError("Expected at least 1 cancellation");
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had at least n subscribers that have been cancelled.
	 *
	 * @param n the expected number of subscribers to have been cancelled.
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertCancelled(int n) {
		int cc = cancelCount;
		if (cc != n) {
			throw new AssertionError("Expected " + n + " cancellations, got " + cc);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had no cancelled subscribers.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertNotCancelled() {
		if (cancelCount != 0) {
			throw new AssertionError("Expected no cancellation");
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had subscriber that saw request overflow,
	 * that is received an onNext event despite having a requested amount of 0 at
	 * the time.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertRequestOverflow() {
		if (!hasOverflown) {
			throw new AssertionError("Expected some request overflow");
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had no subscriber with request overflow.
	 * Request overflow is receiving an onNext event despite having a requested amount
	 * of 0 at that time.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> assertNoRequestOverflow() {
		if (hasOverflown) {
			throw new AssertionError("Unexpected request overflow");
		}
		return this;
	}

	void internalNext(T t) {
		if (!misbehaviors.contains(Misbehavior.ALLOW_NULL)) {
			Objects.requireNonNull(t, "emitted values must be non-null");
		}

		for (AssertPublisherSubcription<T> s : subscribers) {
			s.onNext(t);
		}
	}

	/**
	 * Send 1-n {@link Subscriber#onNext(Object) onNext} signals to the subscribers.
	 *
	 * @param first the first item to emit
	 * @param rest the rest of the items to emit
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> next(T first, T... rest) {
		internalNext(first);
		for (T t : rest) {
			internalNext(t);
		}
		return this;
	}

	/**
	 * Triggers an {@link Subscriber#onError(Throwable) error} signal to the subscribers.
	 *
	 * @param t the {@link Throwable} to trigger
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> error(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		for (AssertPublisherSubcription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onError(t);
		}
		return this;
	}

	/**
	 * Triggers {@link Subscriber#onComplete() completion} of this publisher.
	 *
	 * @return this {@link AssertPublisher} for chaining.
	 */
	public AssertPublisher<T> complete() {
		for (AssertPublisherSubcription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onComplete();
		}
		return this;
	}

	/**
	 * Combine emitting items and completing this publisher.
	 *
	 * @param values the values to emit to subscribers
	 * @return this {@link AssertPublisher} for chaining.
	 * @see #next(Object, Object[]) next
	 * @see #complete() complete
	 */
	public AssertPublisher<T> emit(T... values) {
		for (T t : values) {
			internalNext(t);
		}
		return complete();
	}

}
