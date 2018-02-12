package reactor.adapter.rxjava

import io.reactivex.BackpressureStrategy
import io.reactivex.Completable
import io.reactivex.Flowable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


/**
 * Wraps a Flowable instance into a Flux instance, composing the micro-fusion
 * properties of the Flowable through.
 * @param <T> the value type
 * @return the new Flux instance
 */
fun <T> Flowable<T>.toFlux(): Flux<T> {
    return RxJava2Adapter.flowableToFlux<T>(this)
}

/**
 * Wraps a Flux instance into a Flowable instance, composing the micro-fusion
 * properties of the Flux through.
 * @param <T> the value type
 * @return the new Flux instance
 */
fun <T> Flux<T>.toFlowable(): Flowable<T> {
    return RxJava2Adapter.fluxToFlowable(this)
}

/**
 * Wraps a Mono instance into a Flowable instance, composing the micro-fusion
 * properties of the Flux through.
 * @param <T> the value type
 * @return the new Flux instance
 */
fun <T> Mono<T>.toFlowable(): Flowable<T> {
    return RxJava2Adapter.monoToFlowable<T>(this)
}

/**
 * Wraps a void-Mono instance into a RxJava Completable.
 * @return the new Completable instance
 */
fun Mono<*>.toCompletable(): Completable {
    return RxJava2Adapter.monoToCompletable(this)
}

/**
 * Wraps a RxJava Completable into a Mono instance.
 * @return the new Mono instance
 */
fun Completable.toMono(): Mono<Void> {
    return RxJava2Adapter.completableToMono(this)
}

/**
 * Wraps a Mono instance into a RxJava Single.
 *
 * If the Mono is empty, the single will signal a
 * [NoSuchElementException].
 * @param <T> the value type
 * @return the new Single instance
 */
fun <T> Mono<T>.toSingle(): Single<T> {
    return RxJava2Adapter.monoToSingle(this)
}

/**
 * Wraps a RxJava Single into a Mono instance.
 * @param <T> the value type
 * @return the new Mono instance
 */
fun <T> Single<T>.toMono(): Mono<T> {
    return RxJava2Adapter.singleToMono<T>(this)
}

/**
 * Wraps an RxJava Observable and applies the given backpressure strategy.
 * @param <T> the value type
 * @param strategy the back-pressure strategy, default is BUFFER
 * @return the new Flux instance
 */
fun <T> Observable<T>.toFlux(strategy: BackpressureStrategy = BackpressureStrategy.BUFFER): Flux<T> {
    return RxJava2Adapter.observableToFlux(this, strategy)
}

/**
 * Wraps a Flux instance into a RxJava Observable.
 * @param <T> the value type
 * @return the new Observable instance
 */
fun <T> Flux<T>.toObservable(): Observable<T> {
    return RxJava2Adapter.fluxToFlowable(this).toObservable()
}

/**
 * Wraps an RxJava Maybe into a Mono instance.
 * @param <T> the value type
 * @return the new Mono instance
 */
fun <T> Maybe<T>.toMono(): Mono<T> {
    return RxJava2Adapter.maybeToMono(this)
}

/**
 * WRaps Mono instance into an RxJava Maybe.
 * @param <T> the value type
 * @return the new Maybe instance
 */
fun <T> Mono<T>.toMaybe(): Maybe<T> {
    return RxJava2Adapter.monoToMaybe(this)
}
