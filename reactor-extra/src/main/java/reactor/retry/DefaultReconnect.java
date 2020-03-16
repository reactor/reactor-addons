package reactor.retry;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

public class DefaultReconnect<T, C> extends AbstractRetry<C, T> implements Reconnect<T, C> {

	static final Logger log = Loggers.getLogger(DefaultReconnect.class);

	static final Consumer<? super IterationContext<?>>  NOOP_ON_RECONNECT    = __ -> {};
	static final Predicate<? super IterationContext<?>> ALWAYS_TRY_RECONNECT = __ -> true;
	static final Consumer<?> NOOP_ON_DISPOSE = __ -> {};
	static final BackoffDelay RETRY_PREDICATE = new BackoffDelay(Duration.ofSeconds(-1)) {
		@Override
		public String toString() {
			return "{PREDICATE}";
		}
	};

	final Mono<T>                            source;
	final Predicate<? super RetryContext<C>> reconnectPredicate;
	final Consumer<? super RetryContext<C>>  onReconnect;
	final Consumer<? super T>                onDispose;

	public DefaultReconnect(
			Mono<T> source,
			Predicate<? super RetryContext<C>> repeatPredicate,
			long maxReconnection,
			Duration timeout,
			Backoff backoff,
			Jitter jitter,
			Scheduler backoffScheduler,
			final Consumer<? super RetryContext<C>> onReconnect,
			Consumer<? super T> onDispose,
			C applicationContext) {
		super(maxReconnection, timeout, backoff, jitter, backoffScheduler, applicationContext);
		this.source = source;
		this.reconnectPredicate = repeatPredicate;
		this.onReconnect = onReconnect;
		this.onDispose = onDispose;
	}

	/**
	 * Repeat function that repeats only if the predicate returns true.
	 * @param predicate Predicate that determines if next repeat is performed
	 * @return Repeat function with predicate
	 */
	public Reconnect<T, C> onlyIf(Predicate<? super RetryContext<C>> predicate) {
		return new DefaultReconnect<T, C>(
				this.source,
				predicate,
				this.maxIterations,
				this.timeout,
				this.backoff,
				this.jitter,
				this.backoffScheduler,
				this.onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <NC extends C> Reconnect<T, NC> withApplicationContext(NC applicationContext) {
		return new DefaultReconnect<T, NC>(
				this.source,
				(Predicate) this.reconnectPredicate,
				this.maxIterations,
				this.timeout,
				this.backoff,
				this.jitter,
				this.backoffScheduler,
				(Consumer) this.onReconnect,
				this.onDispose,
				applicationContext
		);
	}

	@Override
	public Reconnect<T, C> doOnReconnect(Consumer<? super RetryContext<C>> onReconnect) {
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				this.maxIterations,
				this.timeout,
				this.backoff,
				this.jitter,
				this.backoffScheduler,
				onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	public Reconnect<T, C> doOnDispose(Consumer<? super T> onDispose) {
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				this.maxIterations,
				this.timeout,
				this.backoff,
				this.jitter,
				this.backoffScheduler,
				this.onReconnect,
				onDispose,
				this.applicationContext
		);
	}

	@Override
	public Reconnect<T, C> timeout(Duration timeout) {
		if (timeout.isNegative())
			throw new IllegalArgumentException("timeout should be >= 0");
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				this.maxIterations,
				timeout,
				this.backoff,
				this.jitter,
				this.backoffScheduler,
				this.onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	public Reconnect<T, C> reconnectMax(long maxReconnects) {
		if (maxReconnects < 1)
			throw new IllegalArgumentException("maxReconnects should be > 0");
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				maxReconnects,
				this.timeout,
				this.backoff,
				this.jitter,
				this.backoffScheduler,
				this.onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	public Reconnect<T, C> backoff(Backoff backoff) {
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				this.maxIterations,
				this.timeout,
				backoff,
				this.jitter,
				this.backoffScheduler,
				this.onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	public Reconnect<T, C> jitter(Jitter jitter) {
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				this.maxIterations,
				this.timeout,
				this.backoff,
				jitter,
				this.backoffScheduler,
				this.onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	public Reconnect<T, C> withBackoffScheduler(Scheduler scheduler) {
		return new DefaultReconnect<T, C>(
				this.source,
				this.reconnectPredicate,
				this.maxIterations,
				this.timeout,
				this.backoff,
				this.jitter,
				scheduler,
				this.onReconnect,
				this.onDispose,
				this.applicationContext
		);
	}

	@Override
	public ReconnectMono<T> build(BiConsumer<? super T, Runnable> resetHook) {
		return new DefaultReconnectMono<>(this, resetHook);
	}


	BackoffDelay reconnectBackoff(long iteration, Instant resetTime,
			Instant timeoutInstant, DefaultContext<T> context, @Nullable Throwable e) {
		DefaultContext<C> tmpContext = new DefaultContext<>(applicationContext, iteration, context.lastBackoff, e);
		BackoffDelay nextBackoff = calculateBackoff(tmpContext, timeoutInstant);
		DefaultContext<C> repeatContext = new DefaultContext<>(applicationContext, iteration, nextBackoff, e);
		context.lastBackoff = nextBackoff;

		if (!reconnectPredicate.test(repeatContext)) {
			log.debug("Stopping repeats since predicate returned false, retry context: {}", repeatContext);
			return RETRY_PREDICATE;
		}
		else if (nextBackoff == RETRY_EXHAUSTED) {
			log.debug("Repeats exhausted, retry context: {}", repeatContext);
			return RETRY_EXHAUSTED;
		}
		else {
			log.debug("Scheduling repeat attempt, retry context: {}", repeatContext);
			onReconnect.accept(repeatContext);
			if (iteration == 0) {
				final Duration between = Duration.between(resetTime, clock.instant());
				final Duration diff = nextBackoff.delay.minus(between);
				return diff.isNegative()
						? BackoffDelay.ZERO
						: diff.compareTo(nextBackoff.delay) > 0
							? nextBackoff
							: new BackoffDelay(diff);
			}
			return nextBackoff;
		}
	}

	@Override
	public Publisher<Long> apply(Flux<T> flux) {
		return Flux.error(new UnsupportedOperationException("Should not be called"));
	}
}
