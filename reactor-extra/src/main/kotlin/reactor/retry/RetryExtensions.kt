package reactor.retry

import reactor.core.publisher.Flux
import java.time.Duration


fun <T> Flux<T>.repeatExponentialBackoff(times: Int, first: Duration, max: Duration? = null,
        jitter: Boolean = false): Flux<T> =
        Repeat.times<T>(times)
                .exponentialBackoff(first, max)
                .jitter(if (jitter) Jitter.random() else Jitter.noJitter())
                .apply(this)

fun <T> Flux<T>.repeatRandomBackoff(times: Int, first: Duration, max: Duration? = null,
        jitter: Boolean = false): Flux<T> =
        Repeat.times<T>(times)
                .randomBackoff(first, max)
                .jitter(if (jitter) Jitter.random() else Jitter.noJitter())
                .apply(this)
