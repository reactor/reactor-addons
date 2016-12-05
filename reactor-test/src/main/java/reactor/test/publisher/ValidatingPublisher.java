package reactor.test.publisher;

import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Operators;

/**
 * A {@link Publisher} that you can directly manipulate, triggering
 * {@link #next(Object, Object[]) onNext}, {@link #complete() onComplete} and
 * {@link #error(Throwable) onError} events, for testing purposes.
 * You can assert the state of the publisher using its {@code expectXXX} methods,
 * usually inside a {@link reactor.test.StepVerifier}'s
 * {@link reactor.test.StepVerifier.Step#then(Runnable) then} callback.
 * <p>
 * The ValidatingPublisher can also be made more lenient towards the RS spec
 * and allow "bad behavior" to be performed, as enumerated in {@link Misbehavior}.
 *
 * @author Simon Baslé
 */
public class ValidatingPublisher<T> implements Publisher<T> {

	/**
	 * Create a {@link Misbehavior misbehaving} {@link ValidatingPublisher}.
	 *
	 * @param first the first allowed {@link Misbehavior}
	 * @param rest additional optional misbehaviors
	 * @param <T> the type of the publisher
	 * @return the new misbehaving {@link ValidatingPublisher}
	 */
	public static <T> ValidatingPublisher<T> createMisbehaving(Misbehavior first, Misbehavior... rest) {
		return new ValidatingPublisher<>(first, rest);
	}

	/**
	 * Create a standard {@link ValidatingPublisher}.
	 *
	 * @param <T> the type of the publisher
	 * @return the new {@link ValidatingPublisher}
	 */
	public static <T> ValidatingPublisher<T> create() {
		return new ValidatingPublisher<T>();
	}

	/**
	 * Possible misbehavior for a {@link ValidatingPublisher}.
	 */
	public enum Misbehavior {
		/**
		 * Allow {@link ValidatingPublisher#next(Object, Object[]) next} calls to be made
		 * despite insufficient request, without triggering an {@link IllegalStateException}.
		 */
		REQUEST_OVERFLOW,
		/**
		 * Allow {@link ValidatingPublisher#next(Object, Object[]) next} calls to be made
		 * with a {@code null} value without triggering a {@link NullPointerException}
		 */
		ALLOW_NULL
	}

	@SuppressWarnings("rawtypes")
	private static final AssertPublisherSubcription[] EMPTY = new AssertPublisherSubcription[0];

	@SuppressWarnings("rawtypes")
	private static final AssertPublisherSubcription[] TERMINATED = new AssertPublisherSubcription[0];

	volatile int cancelCount;

	static final AtomicIntegerFieldUpdater<ValidatingPublisher> CANCEL_COUNT =
		AtomicIntegerFieldUpdater.newUpdater(ValidatingPublisher.class, "cancelCount");

	Throwable error;

	volatile boolean hasOverflown;

	final EnumSet<Misbehavior> misbehaviors;

	@SuppressWarnings("unchecked")
	volatile AssertPublisherSubcription<T>[] subscribers = EMPTY;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ValidatingPublisher, AssertPublisherSubcription[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(ValidatingPublisher.class, AssertPublisherSubcription[].class, "subscribers");

	ValidatingPublisher(Misbehavior first, Misbehavior... rest) {
		this.misbehaviors = EnumSet.of(first, rest);
	}

	ValidatingPublisher() {
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

	static final class AssertPublisherSubcription<T> implements Subscription {

		final Subscriber<? super T> actual;

		final ValidatingPublisher<T> parent;

		volatile boolean cancelled;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AssertPublisherSubcription>
				REQUESTED =
				AtomicLongFieldUpdater.newUpdater(AssertPublisherSubcription.class, "requested");

		public AssertPublisherSubcription(Subscriber<? super T> actual, ValidatingPublisher<T> parent) {
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
				ValidatingPublisher.CANCEL_COUNT.incrementAndGet(parent);
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

	/**
	 * Assert that the current minimum request of all this publisher's subscribers
	 * is &gt;= {@code n}.
	 *
	 * @param n the expected minimum request
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectMinRequested(long n) {
		AssertPublisherSubcription<T>[] subs = subscribers;
		long minRequest = Stream.of(subs)
		                        .mapToLong(s -> s.requested)
		                        .min()
		                        .orElse(0);
		if (minRequest < n) {
			throw new AssertionError("Expected minimum request of " + n + "; got " + minRequest);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has subscribers.
	 *
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectSubscribers() {
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
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectSubscribers(int n) {
		int sl = subscribers.length;
		if (sl != n) {
			throw new AssertionError("Expected " + n + " subscribers, got " + sl);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has no subscribers.
	 *
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectNoSubscribers() {
		int sl = subscribers.length;
		if (sl != 0) {
			throw new AssertionError("Expected no subscribers, got " + sl);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had at least one subscriber that has been cancelled.
	 *
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectCancelled() {
		if (cancelCount == 0) {
			throw new AssertionError("Expected at least 1 cancellation");
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had at least n subscribers that have been cancelled.
	 *
	 * @param n the expected number of subscribers to have been cancelled.
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectCancelled(int n) {
		int cc = cancelCount;
		if (cc != n) {
			throw new AssertionError("Expected " + n + " cancellations, got " + cc);
		}
		return this;
	}

	/**
	 * Asserts that this publisher has had no cancelled subscribers.
	 *
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectNotCancelled() {
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
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectRequestOverflow() {
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
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> expectNoRequestOverflow() {
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
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> next(T first, T... rest) {
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
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> error(Throwable t) {
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
	 * @return this {@link ValidatingPublisher} for chaining.
	 */
	public ValidatingPublisher<T> complete() {
		for (AssertPublisherSubcription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onComplete();
		}
		return this;
	}

	/**
	 * Combine emitting items and completing this publisher.
	 *
	 * @param values the values to emit to subscribers
	 * @return this {@link ValidatingPublisher} for chaining.
	 * @see #next(Object, Object[]) next
	 * @see #complete() complete
	 */
	public ValidatingPublisher<T> emit(T... values) {
		for (T t : values) {
			internalNext(t);
		}
		return complete();
	}

}
