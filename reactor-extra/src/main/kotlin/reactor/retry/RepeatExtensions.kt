package reactor.retry

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.function.Consumer

/**
 * Extension to add a [Flux.repeat] variant to [Flux] that uses an exponential backoff,
 * as enabled by reactor-extra's [Repeat] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param jitter optional flag to activate random jitter in the backoffs
 * @param doOnRepeat optional [Consumer]-like lambda that will receive a [RepeatContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Flux<T>.repeatExponentialBackoff(times: Int, first: Duration, max: Duration? = null,
                                         jitter: Boolean = false,
                                         doOnRepeat: ((RepeatContext<T>) -> Unit)? = null): Flux<T> {
    val repeat = Repeat.times<T>(times)
            .exponentialBackoff(first, max)
            .jitter(if (jitter) Jitter.random() else Jitter.noJitter())

    return if (doOnRepeat == null)
        repeat.apply(this)
    else
        repeat.doOnRepeat(doOnRepeat).apply(this)
}

/**
 * Extension to add a [Flux.repeat] variant to [Flux] that uses a randomized backoff,
 * as enabled by reactor-extra's [Repeat] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param doOnRepeat optional [Consumer]-like lambda that will receive a [RepeatContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Flux<T>.repeatRandomBackoff(times: Int, first: Duration, max: Duration? = null,
                                    doOnRepeat: ((RepeatContext<T>) -> Unit)? = null): Flux<T> {
    val repeat = Repeat.times<T>(times)
            .randomBackoff(first, max)

    return if (doOnRepeat == null)
        repeat.apply(this)
    else
        repeat.doOnRepeat(doOnRepeat).apply(this)
}


/**
 * Extension to add a [Mono.repeat] variant to [Mono] that uses an exponential backoff,
 * as enabled by reactor-extra's [Repeat] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param jitter optional flag to activate random jitter in the backoffs
 * @param doOnRepeat optional [Consumer]-like lambda that will receive a [RepeatContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Mono<T>.repeatExponentialBackoff(times: Int, first: Duration, max: Duration? = null,
                                         jitter: Boolean = false,
                                         doOnRepeat: ((RepeatContext<T>) -> Unit)? = null): Flux<T> {
    val repeat = Repeat.times<T>(times)
            .exponentialBackoff(first, max)
            .jitter(if (jitter) Jitter.random() else Jitter.noJitter())

    return if (doOnRepeat == null)
        repeat.apply(this)
    else
        repeat.doOnRepeat(doOnRepeat).apply(this)
}

/**
 * Extension to add a [Mono.repeat] variant to [Mono] that uses a randomized backoff,
 * as enabled by reactor-extra's [Repeat] builder.
 *
 * @param times the maximum number of retry attempts
 * @param first the initial delay in retrying
 * @param max the optional upper limit the delay can reach after subsequent backoffs
 * @param doOnRepeat optional [Consumer]-like lambda that will receive a [RepeatContext]
 *   each time an attempt is made.
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun <T> Mono<T>.repeatRandomBackoff(times: Int, first: Duration, max: Duration? = null,
                                    doOnRepeat: ((RepeatContext<T>) -> Unit)? = null): Flux<T> {
    val repeat = Repeat.times<T>(times)
            .randomBackoff(first, max)

    return if (doOnRepeat == null)
        repeat.apply(this)
    else
        repeat.doOnRepeat(doOnRepeat).apply(this)
}