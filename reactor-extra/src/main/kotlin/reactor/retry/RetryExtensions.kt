package reactor.retry

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.function.Consumer

/**
 * Extension to add a [Flux.retry] variant to [Flux] that uses an exponential backoff,
 * as enabled by reactor-extra's [Retry] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param jitter optional flag to activate random jitter in the backoffs
 * @param doOnRetry optional [Consumer]-like lambda that will receive a [RetryContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("retryExponentialBackoff(times, first, max, jitter, doOnRetry)", "reactor.kotlin.extra.retry.retryExponentialBackoff"))
fun <T> Flux<T>.retryExponentialBackoff(times: Long, first: Duration, max: Duration? = null,
                                        jitter: Boolean = false,
                                        doOnRetry: ((RetryContext<T>) -> Unit)? = null): Flux<T> {
    val retry = Retry.any<T>()
            .retryMax(times)
            .exponentialBackoff(first, max)
            .jitter(if (jitter) Jitter.random() else Jitter.noJitter())

    return if (doOnRetry == null)
        retry.apply(this)
    else
        retry.doOnRetry(doOnRetry).apply(this)
}

/**
 * Extension to add a [Flux.retry] variant to [Flux] that uses a random backoff,
 * as enabled by reactor-extra's [Retry] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param doOnRetry optional [Consumer]-like lambda that will receive a [RetryContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("retryRandomBackoff(times, first, max, doOnRetry)", "reactor.kotlin.extra.retry.retryRandomBackoff"))
fun <T> Flux<T>.retryRandomBackoff(times: Long, first: Duration, max: Duration? = null,
                                   doOnRetry: ((RetryContext<T>) -> Unit)? = null): Flux<T> {
    val retry = Retry.any<T>()
            .retryMax(times)
            .randomBackoff(first, max)

    return if (doOnRetry == null)
        retry.apply(this)
    else
        retry.doOnRetry(doOnRetry).apply(this)
}

/**
 * Extension to add a [Mono.retry] variant to [Mono] that uses an exponential backoff,
 * as enabled by reactor-extra's [Retry] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param jitter optional flag to activate random jitter in the backoffs
 * @param doOnRetry optional [Consumer]-like lambda that will receive a [RetryContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("retryExponentialBackoff(times, first, max, jitter, doOnRetry)", "reactor.kotlin.extra.retry.retryExponentialBackoff"))
fun <T> Mono<T>.retryExponentialBackoff(times: Long, first: Duration, max: Duration? = null,
                                        jitter: Boolean = false,
                                        doOnRetry: ((RetryContext<T>) -> Unit)? = null): Mono<T> {
    val retry = Retry.any<T>()
            .retryMax(times)
            .exponentialBackoff(first, max)
            .jitter(if (jitter) Jitter.random() else Jitter.noJitter())

    return if (doOnRetry == null)
        this.retryWhen(reactor.util.retry.Retry.withThrowable(retry))
    else
        this.retryWhen(reactor.util.retry.Retry.withThrowable(retry.doOnRetry(doOnRetry)))
}

/**
 * Extension to add a [Mono.retry] variant to [Mono] that uses a random backoff,
 * as enabled by reactor-extra's [Retry] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param doOnRetry optional [Consumer]-like lambda that will receive a [RetryContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("retryRandomBackoff(times, first, max, doOnRetry)", "reactor.kotlin.extra.retry.retryRandomBackoff"))
fun <T> Mono<T>.retryRandomBackoff(times: Long, first: Duration, max: Duration? = null,
                                   doOnRetry: ((RetryContext<T>) -> Unit)? = null): Mono<T> {
    val retry = Retry.any<T>()
            .retryMax(times)
            .randomBackoff(first, max)

    return if (doOnRetry == null)
        this.retryWhen(reactor.util.retry.Retry.withThrowable(retry))
    else
        this.retryWhen(reactor.util.retry.Retry.withThrowable(retry.doOnRetry(doOnRetry)))
}

