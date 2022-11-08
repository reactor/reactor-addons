/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.operators.QueueSubscription;
import io.reactivex.rxjava3.internal.subscribers.BasicFuseableSubscriber;
import io.reactivex.rxjava3.observers.BaseTestConsumer;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import jdk.nashorn.internal.objects.NativeFloat32Array;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class RxJava3AdapterTest {

	@Test
	public void completableToMono() {
		Mono<Void> m = Completable.complete()
		                          .to(RxJava3Adapter::completableToMono);

		StepVerifier.create(m)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void completableToMonoFused() {
		Mono<Void> m = Completable.complete()
		                          .to(RxJava3Adapter::completableToMono);

		StepVerifier.create(m)
		            .expectFusion(Fuseable.ANY, Fuseable.ASYNC)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void flowableToFlux() {
		Flux<Integer> f = Flowable.range(1, 10)
		                          .hide()
		                          .to(RxJava3Adapter::flowableToFlux);

		StepVerifier.create(f)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void flowableToFluxFused() {
		Flux<Integer> f = Flowable.range(1, 10)
		                          .to(RxJava3Adapter::flowableToFlux);

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
		                          .as(RxJava3Adapter::fluxToFlowable);

		StepVerifier.create(f)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void fluxToFlowableRequestFusionGoThrough() throws InterruptedException {
		//this test intentionally uses dummy fusion values to see they propagate through adapters
		final AtomicInteger reactorSeenRequestedMode = new AtomicInteger();
		AtomicInteger rxSeenNegotiatedMode = new AtomicInteger();
		CountDownLatch latch = new CountDownLatch(1);
		//a very naive Flux implementation that will capture the requested fusion mode from rx
		Flux<Integer> flux = Flux.from(subscriber -> {
			subscriber.onSubscribe(new Fuseable.QueueSubscription<Integer>() {

				@Override
				public int requestFusion(int requestedMode) {
					reactorSeenRequestedMode.set(requestedMode);
					return requestedMode + 11;
				}

				@Override
				public int size() {
					return 0;
				}

				@Override
				public boolean isEmpty() {
					return true;
				}

				@Override
				public void clear() {

				}

				@Override
				public void request(long l) {
					subscriber.onComplete();
				}

				@Override
				public void cancel() {

				}

				@Override
				public Integer poll() {
					return null;
				}
			});
		});

		TestSubscriber<Integer> ts = new TestSubscriber<>();
		flux.as(RxJava3Adapter::fluxToFlowable)
		    .subscribe(new BasicFuseableSubscriber<Integer, Integer>(ts) {
			    @Override
			    public Integer poll() throws Throwable {
				    return qs.poll();
			    }

			    @Override
			    public int requestFusion(int mode) {
				    return qs.requestFusion(mode);
			    }

			    @Override
			    protected void afterDownstream() {
				    rxSeenNegotiatedMode.set(qs.requestFusion(30));
			    }

			    @Override
			    public void onNext(Integer integer) { downstream.onNext(integer);}

			    @Override
			    public void onError(Throwable throwable) { downstream.onError(throwable); }

			    @Override
			    public void onComplete() { downstream.onComplete(); }
		    });


		assertThat(rxSeenNegotiatedMode)
				.as("rx requestFusion get answered by reactor")
				.hasValue(41);
		assertThat(reactorSeenRequestedMode)
				.as("reactor saw rx requestFusion")
				.hasValue(30);
		ts.assertComplete();
	}

	@Test
	public void maybeToMonoEmpty() {
		Mono<Void> m = Maybe.<Void>empty().to(RxJava3Adapter::maybeToMono);
		StepVerifier.create(m)
		            .expectComplete()
		            .verify();
	}
	//NB: MonoSubscribers like MaybeToMonoSubscriber are not fuseable anymore

	@Test
	public void maybeToMonoError() {
		Mono<Void> m =
				Maybe.<Void>error(new IOException("Forced failure")).to(RxJava3Adapter::maybeToMono);

		StepVerifier.create(m)
		            .expectErrorMessage("Forced failure")
		            .verify();
	}

	@Test
	public void maybeToMonoJust() {
		Mono<Integer> m = Maybe.just(1)
		                       .to(RxJava3Adapter::maybeToMono);

		StepVerifier.create(m)
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void monoEmptyToSingle() {
		Mono.<Integer>empty().as(RxJava3Adapter::monoToSingle)
		                     .test()
		                     .assertNoValues()
		                     .assertError(NoSuchElementException.class)
		                     .assertNotComplete();
	}

	@Test
	public void monoEmptyToCompletable() {
		Mono.empty()
		    .as(RxJava3Adapter::monoToCompletable)
		    .test()
		    .assertNoValues()
		    .assertNoErrors()
		    .assertComplete();
	}

	@Test
	public void monoValuedToCompletableDiscards() {
		List<String> discarded = new CopyOnWriteArrayList<>();
		Mono.just("foo")
		    .as(m -> RxJava3Adapter.monoToCompletable(m, discarded::add))
		    .test()
		    .assertNoValues()
		    .assertNoErrors()
		    .assertComplete();

		assertThat(discarded).containsExactly("foo");
	}

	@Test
	public void monoToMaybeEmpty() {
		Mono.empty()
		    .as(RxJava3Adapter::monoToMaybe)
		    .test()
		    .assertResult();
	}

	@Test
	public void monoToMaybeError() {
		Mono.error(new IOException("Forced failure"))
		    .as(RxJava3Adapter::monoToMaybe)
		    .test()
		    .assertError(e -> e instanceof IOException && "Forced failure".equals(e.getMessage()));
	}

	@Test
	public void monoToMaybeJust() {
		Mono.just(1)
		    .as(RxJava3Adapter::monoToMaybe)
		    .test()
		    .assertResult(1);
	}

	@Test
	public void monoToSingle() {
		Mono.just(1)
		    .as(RxJava3Adapter::monoToSingle)
		    .test()
		    .assertValues(1)
		    .assertNoErrors()
		    .assertComplete();
	}

	@Test
	public void singleToMono() {
		Mono<Integer> m = Single.just(1)
		                        .to(RxJava3Adapter::singleToMono);

		StepVerifier.create(m)
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}
	//NB: MonoSubscribers like SingleToMonoSubscriber are not fuseable anymore

	@Test
	public void scheduler() {
		Flux<Integer> f = Flux.range(1, 10)
		                      .publishOn(RxJava3Scheduler.from(Schedulers.computation()));

		StepVerifier.create(f)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void schedulerIsDisposed() {
		final AtomicInteger disposed = new AtomicInteger();
		io.reactivex.rxjava3.core.Scheduler foreignScheduler = new io.reactivex.rxjava3.core.Scheduler() {

			@Override
			public void shutdown() {
				disposed.incrementAndGet();
			}

			@Override
			public @NonNull Worker createWorker() {
				return null;
			}
		};
		Scheduler adaptedScheduler = RxJava3Scheduler.from(foreignScheduler);

		assertThat(adaptedScheduler.isDisposed()).as("not disposed").isFalse();

		adaptedScheduler.dispose();
		adaptedScheduler.dispose();
		adaptedScheduler.dispose();

		assertThat(adaptedScheduler.isDisposed()).as("disposed").isTrue();
		assertThat(disposed).as("shutdown only called once").hasValue(1);
	}

	@Test
	public void schedulerCallsStart() {
		final AtomicInteger started = new AtomicInteger();
		io.reactivex.rxjava3.core.Scheduler foreignScheduler = new io.reactivex.rxjava3.core.Scheduler() {

			@Override
			public void start() {
				started.incrementAndGet();
			}

			@Override
			public @NonNull Worker createWorker() {
				return null;
			}
		};
		Scheduler adaptedScheduler = RxJava3Scheduler.from(foreignScheduler);

		adaptedScheduler.start();

		assertThat(started).as("start is propagated").hasValue(1);
	}
}
