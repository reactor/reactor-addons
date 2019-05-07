package reactor.adapter.rxjava

import io.reactivex.Completable
import io.reactivex.Flowable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import org.junit.Test
import reactor.core.Fuseable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.test
import java.util.*


class RxJava2AdapterExtTest {

    @Test
    fun `Flowable to Flux`() {
        Flowable.range(1, 10)
            .toFlux()
            .test()
            .expectFusion()
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .expectComplete()
            .verify()
    }

    @Test
    fun `Flux to Flowable`() {
        Flux.range(1, 10)
            .toFlowable()
            .test()
            .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .assertComplete()
    }

    @Test
    fun `Mono to Flowable`() {
        Mono.just(1)
            .toFlowable()
            .test()
            .assertValues(1)
            .assertNoErrors()
            .assertComplete()
    }

    @Test
    fun `Mono to Completable`() {
        Mono.empty<Any>()
            .toCompletable()
            .test()
            .assertNoValues()
            .assertNoErrors()
            .assertComplete()
    }

    @Test
    fun `Completable to Mono`() {
        Completable.complete()
            .toMono()
            .test()
            .expectComplete()
            .verify()
    }

    @Test
    fun `Mono to Single`() {
        Mono.just(1)
            .toSingle()
            .test()
            .assertValues(1)
            .assertNoErrors()
            .assertComplete()
    }

    @Test
    fun `Empty Mono to Single`() {
        Mono.empty<Int>()
            .toSingle()
            .test()
            .assertNoValues()
            .assertError(NoSuchElementException::class.java)
            .assertNotComplete()
    }

    @Test
    fun `Single to Mono`() {
        Single.just(1)
            .toMono()
            .test()
            .expectFusion(Fuseable.ANY, Fuseable.ASYNC)
            .expectNext(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `Observable to Flux`() {
        Observable.range(1, 10)
            .toFlux()
            .test()
            .expectFusion()
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .expectComplete()
            .verify()
    }

    @Test
    fun `Flux to Observable`() {
        Flux.range(1, 10)
            .toObservable()
            .test()
            .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .assertNoErrors()
            .assertComplete()
    }

    @Test
    fun `Maybe to Mono`() {
        Maybe.just(1)
            .toMono()
            .test()
            .expectNext(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `Empty Maybe to Mono`() {
        Maybe.empty<Void>()
            .toMono()
            .test()
            .expectComplete()
            .verify()
    }

    @Test
    fun `Mono to Maybe`() {
        Mono.just(1)
            .toMaybe()
            .test()
            .assertResult(1)
    }

    @Test
    fun `Empty Mono to Maybe`() {
        Mono.empty<Any>()
            .toMaybe()
            .test()
            .assertResult()
    }
}