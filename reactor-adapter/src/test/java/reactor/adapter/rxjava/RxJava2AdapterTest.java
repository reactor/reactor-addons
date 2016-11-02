/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.adapter.rxjava;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.NoSuchElementException;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.internal.fuseable.QueueSubscription;
import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;

public class RxJava2AdapterTest {
    @Test
    public void flowableToFlux() {
	    Flux<Integer> f = Flowable.range(1, 10)
	                              .hide()
	                              .to(RxJava2Adapter::flowableToFlux);

	    StepVerifier.create(f)
	                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	                .expectComplete()
	                .verify();
    }

	@Test
	public void scheduler() {
		Flux<Integer> f = Flux.range(1, 10)
		                      .publishOn(RxJava2Scheduler.from(Schedulers.computation()));

		StepVerifier.create(f)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .expectComplete()
		            .verify();
	}

    @Test
    public void flowableToFluxFused() {
	    Flux<Integer> f = Flowable.range(1, 10)
	                              .to(RxJava2Adapter::flowableToFlux);

	    StepVerifier.create(f)
	                .expectFusion()
	                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	                .expectComplete()
	                .verify();
    }

    @Test
    public void fluxToFlowable() {
	    Flowable<Integer> f = Flux.range(1, 10)
	                              .hide()
	                              .as(RxJava2Adapter::fluxToFlowable);

	    StepVerifier.create(f)
	                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	                .expectComplete()
	                .verify();
    }

    @Test
    public void fluxToFlowableFused() throws Exception {
        io.reactivex.subscribers.TestSubscriber<Integer> ts = new io.reactivex.subscribers.TestSubscriber<>();
        
        // Testing for fusion is an RxJava 2 internal affair unfortunately
	    Field f = BaseTestConsumer.class.getDeclaredField("initialFusionMode");
	    f.setAccessible(true);
	    f.setInt(ts, QueueSubscription.ANY);
        
        Flux.range(1, 10)
        .as(RxJava2Adapter::fluxToFlowable)
        .subscribe(ts);
        
        ts
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoErrors()
        .assertComplete();
        
        // Testing for fusion is an RxJava 2 internal affair unfortunately
	    f = BaseTestConsumer.class.getDeclaredField("establishedFusionMode");
	    f.setAccessible(true);
	    assertEquals(QueueSubscription.SYNC, f.getInt(ts));
    }
    
    @Test
    public void singleToMono() {
	    Mono<Integer> m = Single.just(1)
	                            .to(RxJava2Adapter::singleToMono);

	    StepVerifier.create(m)
	                .expectNext(1)
	                .expectComplete()
	                .verify();
    }

    @Test
    public void singleToMonoFused() {
	    Mono<Integer> m = Single.just(1)
	                            .to(RxJava2Adapter::singleToMono);

	    StepVerifier.create(m)
	                .expectFusion(Fuseable.ANY, Fuseable.ASYNC)
	                .expectNext(1)
	                .expectComplete()
	                .verify();
    }
    
    @Test
    public void monoToSingle() {
        Mono.just(1)
        .as(RxJava2Adapter::monoToSingle)
        .test()
        .assertValues(1)
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void monoEmptyToSingle() {
        Mono.<Integer>empty()
        .as(RxJava2Adapter::monoToSingle)
        .test()
        .assertNoValues()
        .assertError(NoSuchElementException.class)
        .assertNotComplete();
    }

    @Test
    public void completableToMono() {
	    Mono<Void> m = Completable.complete()
	                              .to(RxJava2Adapter::completableToMono);

	    StepVerifier.create(m)
	                .expectComplete()
	                .verify();
    }

    @Test
    public void completableToMonoFused() {
	    Mono<Void> m = Completable.complete()
	                              .to(RxJava2Adapter::completableToMono);

	    StepVerifier.create(m)
	                .expectFusion(Fuseable.ANY, Fuseable.ASYNC)
	                .expectComplete()
	                .verify();
    }
    
    @Test
    public void monoToCompletable() {
        Mono.empty()
        .as(RxJava2Adapter::monoToCompletable)
        .test()
        .assertNoValues()
        .assertNoErrors()
        .assertComplete();
    }
    
    @Test
    public void monoToMaybeJust() {
        Mono.just(1)
        .as(RxJava2Adapter::monoToMaybe)
        .test()
        .assertResult(1);
    }
    
    @Test
    public void monoToMaybeEmpty() {
        Mono.empty()
        .as(RxJava2Adapter::monoToMaybe)
        .test()
        .assertResult();
    }
    
    @Test
    public void monoToMaybeError() {
        Mono.error(new IOException("Forced failure"))
        .as(RxJava2Adapter::monoToMaybe)
        .test()
        .assertFailureAndMessage(IOException.class, "Forced failure");
    }
    
    @Test
    public void maybeToMonoJust() {
	    Mono<Integer> m = Maybe.just(1)
	                           .to(RxJava2Adapter::maybeToMono);

	    StepVerifier.create(m)
	                .expectNext(1)
	                .expectComplete()
	                .verify();
    }
    
    @Test
    public void maybeToMonoEmpty() {
	    Mono<Void> m = Maybe.<Void>empty().to(RxJava2Adapter::maybeToMono);
	    StepVerifier.create(m)
	                .expectComplete()
	                .verify();
    }
    
    @Test
    public void maybeToMonoError() {
	    Mono<Void> m =
			    Maybe.<Void>error(new IOException("Forced failure")).to(RxJava2Adapter::maybeToMono);

	    StepVerifier.create(m)
	                .expectErrorMessage("Forced failure")
	                .verify();
    }

    @Test
    public void maybeToMonoEmptyFused() {
	    Mono<Void> m = Maybe.<Void>empty().to(RxJava2Adapter::maybeToMono);

	    StepVerifier.create(m)
	                .expectFusion(Fuseable.ANY, Fuseable.ASYNC)
	                .expectComplete()
	                .verify();
    }
}
