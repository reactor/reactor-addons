package reactor.retry

import reactor.core.publisher.Flux
import java.time.Duration

fun <T> Flux<T>.retryExponentialBackoff(times: Int, first: Duration, max: Duration? = null,
                                        jitter: Boolean = false): Flux<T> =
        Retry.any<T>()
                .retryMax(times)
                .exponentialBackoff(first, max)
                .jitter(if (jitter) Jitter.random() else Jitter.noJitter())
                .apply(this)

fun <T> Flux<T>.retryRandomBackoff(times: Int, first: Duration, max: Duration? = null,
                                   jitter: Boolean = false): Flux<T> =
        Retry.any<T>()
                .retryMax(times)
                .randomBackoff(first, max)
                .jitter(if (jitter) Jitter.random() else Jitter.noJitter())
                .apply(this)