package reactor.test.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link Publisher} that you can directly manipulate, triggering
 * {@link #next(Iterable) onNext}, {@link #complete() onComplete} and
 * {@link #error(Throwable) onError} events, for testing purposes.
 * You can assert the state of the publisher using its {@code assertXXX} methods,
 * usually inside a {@link reactor.test.StepVerifier}'s
 * {@link reactor.test.StepVerifier.Step#then(Runnable) then} callback.
 * <p>
 * The TestPublisher can also be made more lenient towards the RS spec
 * and allow "spec violations", as enumerated in {@link Violation}. Use the
 * {@link #createNoncompliant(Violation, Violation...)} factory method to create such
 * a misbehaving publisher.
 *
 * @author Simon Baslé
 */
public interface TestPublisher<T> extends Publisher<T> {

	/**
	 * Create a standard {@link DefaultTestPublisher}.
	 *
	 * @param <T> the type of the publisher
	 * @return the new {@link TestPublisher}
	 */
	static <T> DefaultTestPublisher<T> create() {
		return new DefaultTestPublisher<>();
	}

	/**
	 * Create a {@link Violation noncompliant} {@link DefaultTestPublisher}
	 * with a given set of reactive streams spec violations that will be overlooked.
	 *
	 * @param first the first allowed {@link Violation}
	 * @param rest additional optional violations
	 * @param <T> the type of the publisher
	 * @return the new noncompliant {@link TestPublisher}
	 */
	static <T> DefaultTestPublisher<T> createNoncompliant(Violation first, Violation... rest) {
		return new DefaultTestPublisher<>(first, rest);
	}

	/**
	 * Convenience method to wrap this {@link TestPublisher} to a {@link Flux}.
	 */
	Flux<T> flux();

	/**
	 * Convenience method to wrap this {@link TestPublisher} to a {@link Mono}.
	 */
	Mono<T> mono();

	/**
	 * Assert that the current minimum request of all this publisher's subscribers
	 * is &gt;= {@code n}.
	 *
	 * @param n the expected minimum request
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertMinRequested(long n);

	/**
	 * Asserts that this publisher has subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertSubscribers();

	/**
	 * Asserts that this publisher has exactly n subscribers.
	 *
	 * @param n the expected number of subscribers
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertSubscribers(int n);

	/**
	 * Asserts that this publisher has no subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertNoSubscribers();

	/**
	 * Asserts that this publisher has had at least one subscriber that has been cancelled.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertCancelled();

	/**
	 * Asserts that this publisher has had at least n subscribers that have been cancelled.
	 *
	 * @param n the expected number of subscribers to have been cancelled.
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertCancelled(int n);

	/**
	 * Asserts that this publisher has had no cancelled subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertNotCancelled();

	/**
	 * Asserts that this publisher has had subscriber that saw request overflow,
	 * that is received an onNext event despite having a requested amount of 0 at
	 * the time.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertRequestOverflow();

	/**
	 * Asserts that this publisher has had no subscriber with request overflow.
	 * Request overflow is receiving an onNext event despite having a requested amount
	 * of 0 at that time.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> assertNoRequestOverflow();

	/**
	 * Send 1-n {@link Subscriber#onNext(Object) onNext} signals to the subscribers.
	 *
	 * @param values the items to emit
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> next(Iterable<T> values);

	/**
	 * Triggers an {@link Subscriber#onError(Throwable) error} signal to the subscribers.
	 *
	 * @param t the {@link Throwable} to trigger
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> error(Throwable t);

	/**
	 * Triggers {@link Subscriber#onComplete() completion} of this publisher.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	TestPublisher<T> complete();

	/**
	 * Combine emitting items and completing this publisher.
	 *
	 * @param values the values to emit to subscribers
	 * @return this {@link TestPublisher} for chaining.
	 * @see #next(Iterable) next
	 * @see #complete() complete
	 */
	TestPublisher<T> emit(Iterable<T> values);

	/**
	 * Possible misbehavior for a {@link TestPublisher}.
	 */
	enum Violation {
		/**
		 * Allow {@link TestPublisher#next(Iterable) next} calls to be made
		 * despite insufficient request, without triggering an {@link IllegalStateException}.
		 */
		REQUEST_OVERFLOW,
		/**
		 * Allow {@link TestPublisher#next(Iterable) next} calls to be made
		 * with a {@code null} value without triggering a {@link NullPointerException}
		 */
		ALLOW_NULL
	}
}
