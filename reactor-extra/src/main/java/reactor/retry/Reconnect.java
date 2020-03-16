package reactor.retry;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public interface Reconnect<T, C> {

	@SuppressWarnings("unchecked")
	static <T, C> Reconnect<T, C> fromSource(Mono<T> source) {
		return new DefaultReconnect<T, C>(
				source,
				DefaultReconnect.ALWAYS_TRY_RECONNECT,
				0L,
				null,
				Backoff.ZERO_BACKOFF,
				Jitter.noJitter(),
				null,
				DefaultReconnect.NOOP_ON_RECONNECT,
				(Consumer<? super T>) DefaultReconnect.NOOP_ON_DISPOSE,
				(C) null
		);
	}

	/**
	 * Repeat function that repeats only if the predicate returns true.
	 * @param predicate Predicate that determines if next repeat is performed
	 * @return Repeat function with predicate
	 */
	Reconnect<T, C> onlyIf(Predicate<? super RetryContext<C>> predicate);

	/**
	 * Returns a repeat function with an application context that may be
	 * used to perform any rollbacks before a repeat. This application
	 * context is provided to any repeat predicate {@link #onlyIf(Predicate)},
	 * custom backoff function {@link #backoff(Backoff)} and repeat
	 * callback {@link #doOnRepeat(Consumer)}. All other properties of
	 * this repeat function are retained in the returned instance.
	 *
	 * @param applicationContext Application context
	 * @return repeat function with associated application context
	 */
	<NC extends C> Reconnect<T, NC> withApplicationContext(NC applicationContext);

	/**
	 * Returns a repeat function that invokes the provided onRepeat
	 * callback before every repeat. The {@link RepeatContext} provided
	 * to the callback contains the iteration and the any application
	 * context set using {@link #withApplicationContext(Object)}.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 *
	 * @param onRepeat callback to invoke before repeats
	 * @return repeat function with callback
	 */
	Reconnect<T, C> doOnReconnect(Consumer<? super RetryContext<C>> onReconnect);

	Reconnect<T, C> doOnDispose(Consumer<? super T> onDispose);

	/**
	 * Returns a repeat function with timeout. The timeout starts from
	 * the instant that this function is applied and switches to unlimited
	 * number of attempts. All other properties of
	 * this repeat function are retained in the returned instance.
	 *
	 * @param timeout timeout after which no new repeats are initiated
	 * @return repeat function with timeout
	 */
	Reconnect<T, C> timeout(Duration timeout);

	/**
	 * Returns a repeat function that repeats at most n times. All other
	 * properties of this repeat function are retained in the returned instance.
	 *
	 * @param maxRepeats number of repeats
	 * @return Retry function for n repeats
	 */
	Reconnect<T, C> reconnectMax(long maxReconnects);

	/**
	 * Returns a repeat function with backoff delay.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 *
	 * @param backoff the backoff function to determine backoff delay
	 * @return repeat function with backoff
	 */
	Reconnect<T, C> backoff(Backoff backoff);

	/**
	 * Returns a repeat function that applies jitter to the backoff delay.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 *
	 * @param jitter Jitter function to randomize backoff delay
	 * @return repeat function with jitter for backoff
	 */
	Reconnect<T, C> jitter(Jitter jitter);

	/**
	 * Returns a repeat function that uses the scheduler provided for
	 * backoff delays. All other properties of this repeat function
	 * are retained in the returned instance.
	 * @param scheduler the scheduler for backoff delays
	 * @return repeat function with backoff scheduler
	 */
	Reconnect<T, C> withBackoffScheduler(Scheduler scheduler);

	/**
	 * Returns a repeat function with no backoff delay. This is the default.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 *
	 * @return repeat function with no backoff delay
	 */
	default Reconnect<T, C> noBackoff() {
		return backoff(Backoff.zero());
	}

	/**
	 * Returns a repeat function with fixed backoff delay.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 *
	 * @param backoffInterval fixed backoff delay applied before every repeat
	 * @return repeat function with fixed backoff delay
	 */
	default Reconnect<T, C> fixedBackoff(Duration backoffInterval) {
		return backoff(Backoff.fixed(backoffInterval));
	}

	/**
	 * Returns a repeat function with exponential backoff delay.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 * <p>
	 * Repeats are performed after a backoff interval of <code>firstBackoff * (2 ** n)</code>
	 * where n is the next iteration number. If <code>maxBackoff</code> is not null, the maximum
	 * backoff applied will be limited to <code>maxBackoff</code>.
	 *
	 * @param firstBackoff the delay for the first backoff, which is also used as the coefficient for subsequent backoffs
	 * @param maxBackoff the maximum backoff delay before a repeat
	 * @return repeat function with exponential backoff delay
	 */
	default Reconnect<T, C> exponentialBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 2, false));
	}

	/**
	 * Returns a repeat function with full jitter backoff strategy.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 * <p>
	 * Repeats are performed after a random backoff interval between  <code>firstBackoff</code> and
	 * <code>firstBackoff * (2 ** n)</code> where n is the next iteration number. If <code>maxBackoff</code>
	 * is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 *
	 * @param firstBackoff the delay for the first backoff, which is also used as the coefficient for subsequent backoffs
	 * @param maxBackoff the maximum backoff delay before a repeat
	 * @return repeat function with full jitter backoff strategy
	 */
	default Reconnect<T, C> exponentialBackoffWithJitter(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 2, false)).jitter(Jitter.random());
	}

	/**
	 * Returns a repeat function with random de-correlated jitter backoff strategy.
	 * All other properties of this repeat function are retained in the
	 * returned instance.
	 * <p>
	 * Repeats are performed after a backoff interval of <code>random_between(firstBackoff, prevBackoff * 3)</code>,
	 * with a minimum value of <code>firstBackoff</code>. If <code>maxBackoff</code>
	 * is not null, the maximum backoff applied will be limited to <code>maxBackoff</code>.
	 *
	 * @param firstBackoff the delay for the first backoff, also used as minimum backoff
	 * @param maxBackoff the maximum backoff delay before a repeat
	 * @return repeat function with de-correlated jitter backoff strategy
	 */
	default Reconnect<T, C> randomBackoff(Duration firstBackoff, Duration maxBackoff) {
		return backoff(Backoff.exponential(firstBackoff, maxBackoff, 3, true)).jitter(Jitter.random());
	}

	Mono<T> build(BiConsumer<? super T, Runnable> resetHook);
}
