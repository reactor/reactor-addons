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
fun <T> Flux<T>.retryExponentialBackoff(times: Int, first: Duration, max: Duration? = null,
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
fun <T> Flux<T>.retryRandomBackoff(times: Int, first: Duration, max: Duration? = null,
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
fun <T> Mono<T>.retryExponentialBackoff(times: Int, first: Duration, max: Duration? = null,
                                        jitter: Boolean = false,
                                        doOnRetry: ((RetryContext<T>) -> Unit)? = null): Mono<T> {


    val retry = Retry.any<T>()
            .retryMax(times)
            .exponentialBackoff(first, max)
            .jitter(if (jitter) Jitter.random() else Jitter.noJitter())

    return if (doOnRetry == null)
        this.retryWhen(retry)
    else
        this.retryWhen(retry.doOnRetry(doOnRetry))
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
fun <T> Mono<T>.retryRandomBackoff(times: Int, first: Duration, max: Duration? = null,
                                   doOnRetry: ((RetryContext<T>) -> Unit)? = null): Mono<T> {
    val retry = Retry.any<T>()
            .retryMax(times)
            .randomBackoff(first, max)

    return if (doOnRetry == null)
        this.retryWhen(retry)
    else
        this.retryWhen(retry.doOnRetry(doOnRetry))
}

