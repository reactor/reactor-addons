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

import java.util.NoSuchElementException;
import java.util.function.Consumer;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.MaybeObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleObserver;
import io.reactivex.rxjava3.internal.operators.completable.CompletableFromPublisher;
import io.reactivex.rxjava3.internal.operators.single.SingleFromPublisher;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Operators.MonoSubscriber;

/**
 * Convert between RxJava 3 types and Mono/Flux back and forth and compose backpressure,
 * cancellation and fusion where applicable.
 */
public abstract class RxJava3Adapter {
    /**
     * Adapt an Rxjava3 {@link Flowable} instance into a {@link Flux} instance, allowing micro-fusion
     * and backpressure propagation to occur between the two libraries.
     *
     * @param <T> the value type
     * @param source the source {@link Flowable}
     * @return the new {@link Flux} instance
     */
    public static <T> Flux<T> flowableToFlux(Flowable<T> source) {
        // due to RxJava's own hooks, there is no matching of scalar- and callable types
        // as it would lose tracking information
        return new FlowableAsFlux<>(source);
    }

    /**
     * Adapt a {@link Flux} instance into an Rxjava3 {@link Flowable} instance, allowing micro-fusion
     * and backpressure propagation to occur between the two libraries.
     *
     * @param <T> the value type
     * @param source the source {@link Flux}
     * @return the new {@link Flowable} instance
     */
    public static <T> Flowable<T> fluxToFlowable(Flux<T> source) {
        return new FluxAsFlowable<>(source);
    }

    /**
     * Adapt a {@link Mono} instance into an Rxjava3 {@link Flowable} instance, allowing micro-fusion
     * and backpressure propagation to occur between the two libraries.
     *
     * @param <T> the value type
     * @param source the source {@link Mono}
     * @return the new {@link Flowable} instance
     */
    public static <T> Flowable<T> monoToFlowable(Mono<T> source) {
        return new FluxAsFlowable<>(source);
    }

    /**
     * Adapt a {@link Mono} instance into an Rxjava3 {@link Completable} instance.
     * <p>
     * Unlike the {@link #monoToCompletable(Mono, Consumer)} variant, this one doesn't
     * perform any specific discarding of the emitted value (if there is one).
     *
     * @param source the source {@link Mono}, which value to ignore
     * @return the new {@link Completable} instance
     * @see Mono#ignoreElement()
     * @see #monoToCompletable(Mono, Consumer)
     */
    public static Completable monoToCompletable(Mono<?> source) {
        return new CompletableFromPublisher<>(source);
    }

    /**
     * Adapt a {@link Mono} instance into an Rxjava3 {@link Completable} instance, discarding
     * potential mono value through the provided {@link Consumer} (to be used in a
     * similar fashion to {@link Mono#doOnDiscard(Class, Consumer)} hook). The consumer
     * MUST complete normally, without throwing any exceptions.
     *
     * @param source the source {@link Mono}, which value to ignore and discard
     * @return the new {@link Completable} instance
     * @see Mono#ignoreElement()
     * @see #monoToCompletable(Mono)
     */
    public static <T> Completable monoToCompletable(Mono<T> source, Consumer<T> discardHandler) {
        return new Completable3FromCorePublisher<>(source, discardHandler);
    }

    /**
     * Adapt an Rxjava3 {@link Completable} into a {@link Mono Mono&lt;Void&gt;} instance.
     *
     * @param source the source {@link Completable}
     * @return the new {@link Mono} instance
     */
    public static Mono<Void> completableToMono(Completable source) {
        return new CompletableAsMono(source);
    }

    /**
     * Adapt a {@link Mono} instance into an Rxjava3 {@link Single}.
     * <p>
     * If the Mono is empty, the single will signal a {@link NoSuchElementException}.
     *
     * @param <T> the value type
     * @param source the source {@link Mono} instance
     * @return the new {@link Single} instance
     */
    public static <T> Single<T> monoToSingle(Mono<T> source) {
        return new SingleFromPublisher<>(source);
    }

    /**
     * Adapt an Rxjava3 {@link Single} into a {@link Mono} instance.
     *
     * @param <T> the value type
     * @param source the source {@link Single}
     * @return the new {@link Mono} instance
     */
    public static <T> Mono<T> singleToMono(Single<T> source) {
        return new SingleAsMono<>(source);
    }

    /**
     * Adapt an RxJava 3 {@link Observable} into a {@link Flux} and let that flux deal with
     * backpressure by first turning it into a {@link Flowable} with the given RxJava3
     * {@link BackpressureStrategy}.
     *
     * @param <T> the value type
     * @param source the source {@link Observable}
     * @param strategy the {@link BackpressureStrategy}
     * @return the new {@link Flux} instance
     */
    public static <T> Flux<T> observableToFlux(Observable<T> source, BackpressureStrategy strategy) {
        return flowableToFlux(source.toFlowable(strategy));
    }

    /**
     * Adapt a {@link Flux} instance into an Rxjava3 {@link java.util.Observable}.
     *
     * @param <T> the value type
     * @param source the source {@link Flux}
     * @return the new {@link java.util.Observable} instance
     */
    public static <T> Observable<T> fluxToObservable(Flux<T> source) {
        return fluxToFlowable(source).toObservable();
    }

    /**
     * Adapt an RxJava 3 {@link Maybe} into a {@link Mono} instance.
     *
     * @param <T> the value type
     * @param source the source {@link Maybe}
     * @return the new {@link Mono} instance
     */
    public static <T> Mono<T> maybeToMono(Maybe<T> source) {
        return new MaybeAsMono<>(source);
    }

    /**
     * Adapt a {@link Mono} instance into an RxJava 3 {@link Maybe}.
     *
     * @param <T> the value type
     * @param source the source {@link Mono}
     * @return the new {@link Maybe} instance
     */
    public static <T> Maybe<T> monoToMaybe(Mono<T> source) {
        return new MonoAsMaybe<>(source);
    }

    static final class FlowableAsFlux<T> extends Flux<T> implements Fuseable {

        final Flowable<T> source;

        public FlowableAsFlux(Flowable<T> source) {
            this.source = source;
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> s) {
            if (s instanceof ConditionalSubscriber) {
                source.subscribe(new FlowableAsFluxConditionalSubscriber<>((ConditionalSubscriber<? super T>)s));
            } else {
                source.subscribe(new FlowableAsFluxSubscriber<>(s));
            }
        }

        static final class FlowableAsFluxSubscriber<T> implements FlowableSubscriber<T>, QueueSubscription<T> {

            final Subscriber<? super T> actual;

            Subscription s;

            io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T> qs;

            public FlowableAsFluxSubscriber(Subscriber<? super T> actual) {
                this.actual = actual;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (Operators.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof io.reactivex.rxjava3.internal.fuseable.QueueSubscription) {
                        this.qs = (io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T>)s;
                    }

                    actual.onSubscribe(this);
                }
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public void request(long n) {
                s.request(n);
            }

            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                try {
                    return qs.poll();
                } catch (Throwable ex) {
                    throw Exceptions.bubble(ex);
                }
            }

            @Override
            public int size() {
                return qs.isEmpty() ? 0 : 1; // not really supported by SimpleQueue
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }
        }

        static final class FlowableAsFluxConditionalSubscriber<T> implements
        io.reactivex.rxjava3.internal.fuseable.ConditionalSubscriber<T>, QueueSubscription<T> {

            final ConditionalSubscriber<? super T> actual;

            Subscription s;

            io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T> qs;

            public FlowableAsFluxConditionalSubscriber(ConditionalSubscriber<? super T> actual) {
                this.actual = actual;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (Operators.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof io.reactivex.rxjava3.internal.fuseable.QueueSubscription) {
                        this.qs = (io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T>)s;
                    }

                    actual.onSubscribe(this);
                }
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public boolean tryOnNext(T t) {
                return actual.tryOnNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public void request(long n) {
                s.request(n);
            }

            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                try {
                    return qs.poll();
                } catch (Throwable ex) {
                    throw Exceptions.bubble(ex);
                }
            }

            @Override
            public int size() {
                return qs.isEmpty() ? 0 : 1; // not really supported by SimpleQueue
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }
        }
    }

    static final class FluxAsFlowable<T> extends Flowable<T> {

        final Publisher<T> source;

        public FluxAsFlowable(Publisher<T> source) {
            this.source = source;
        }

        @Override
        public void subscribeActual(Subscriber<? super T> s) {
            if (s instanceof io.reactivex.rxjava3.internal.fuseable.ConditionalSubscriber) {
                source.subscribe(new FluxAsFlowableConditionalSubscriber<>((io.reactivex.rxjava3.internal.fuseable.ConditionalSubscriber<? super T>)s));
            } else {
                source.subscribe(new FluxAsFlowableSubscriber<>(s));
            }
        }

        static final class FluxAsFlowableSubscriber<T> implements CoreSubscriber<T>,
        io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T> {

            final Subscriber<? super T> actual;

            Subscription s;

            Fuseable.QueueSubscription<T> qs;

            public FluxAsFlowableSubscriber(Subscriber<? super T> actual) {
                this.actual = actual;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (Operators.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof Fuseable.QueueSubscription) {
                        this.qs = (Fuseable.QueueSubscription<T>)s;
                    }

                    actual.onSubscribe(this);
                }
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public void request(long n) {
                s.request(n);
            }

            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                return qs.poll();
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return Fuseable.NONE;
            }

            @Override
            public boolean offer(T value) {
                throw new UnsupportedOperationException("Should not be called");
            }

            @Override
            public boolean offer(T v1, T v2) {
                throw new UnsupportedOperationException("Should not be called");
            }
        }

        static final class FluxAsFlowableConditionalSubscriber<T> implements
        Fuseable.ConditionalSubscriber<T>, io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T> {

            final io.reactivex.rxjava3.internal.fuseable.ConditionalSubscriber<? super T> actual;

            Subscription s;

            io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T> qs;

            public FluxAsFlowableConditionalSubscriber(io.reactivex.rxjava3.internal.fuseable.ConditionalSubscriber<? super T> actual) {
                this.actual = actual;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (Operators.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof io.reactivex.rxjava3.internal.fuseable.QueueSubscription) {
                        this.qs = (io.reactivex.rxjava3.internal.fuseable.QueueSubscription<T>)s;
                    }

                    actual.onSubscribe(this);
                }
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public boolean tryOnNext(T t) {
                return actual.tryOnNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public void request(long n) {
                s.request(n);
            }

            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                try {
                    return qs.poll();
                } catch (Throwable ex) {
                    throw Exceptions.bubble(ex);
                }
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }

            @Override
            public boolean offer(T v1) {
                throw new UnsupportedOperationException("Should not be called!");
            }

            @Override
            public boolean offer(T v1, T v2) {
                throw new UnsupportedOperationException("Should not be called!");
            }
        }
    }

    static final class CompletableAsMono extends Mono<Void> implements Fuseable {

        final Completable source;

        public CompletableAsMono(Completable source) {
            this.source = source;
        }

        @Override
        public void subscribe(CoreSubscriber<? super Void> s) {
            source.subscribe(new CompletableAsMonoSubscriber(s));
        }

        static final class CompletableAsMonoSubscriber implements CompletableObserver,
                                                                  QueueSubscription<Void> {

            final Subscriber<? super Void> actual;

            io.reactivex.rxjava3.disposables.Disposable d;

            public CompletableAsMonoSubscriber(Subscriber<? super Void> actual) {
                this.actual = actual;
            }


            @Override
            public void onSubscribe(@NonNull io.reactivex.rxjava3.disposables.Disposable d) {
                this.d = d;
                actual.onSubscribe(this);
            }

            @Override
            public void onError(Throwable e) {
                actual.onError(e);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public void request(long n) {
                // no-op as Completable never signals any value
            }

            @Override
            public void cancel() {
                d.dispose();
            }

            @Override
            public boolean isEmpty() {
                return true;
            }

            @Override
            public Void poll() {
                return null; // always empty
            }

            @Override
            public int requestFusion(int requestedMode) {
                return requestedMode & Fuseable.ASYNC;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public void clear() {
                // nothing to clear
            }
        }
    }

    static final class SingleAsMono<T> extends Mono<T> implements Fuseable {

        final Single<T> source;

        public SingleAsMono(Single<T> source) {
            this.source = source;
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> s) {
            SingleObserver<? super T> single = new SingleAsMonoSubscriber<>(s);
            source.subscribe(single);
        }

        static final class SingleAsMonoSubscriber<T> extends MonoSubscriber<T, T>
                implements SingleObserver<T> {

            io.reactivex.rxjava3.disposables.Disposable d;

            public SingleAsMonoSubscriber(CoreSubscriber<? super T> subscriber) {
                super(subscriber);
            }

            @Override
            public void onSubscribe(io.reactivex.rxjava3.disposables.Disposable d) {
                this.d = d;
                actual.onSubscribe(this);
            }

            @Override
            public void onSuccess(T value) {
                complete(value);
            }
        }
    }

    static final class MonoAsMaybe<T> extends Maybe<T> {
        final Mono<T> source;

        public MonoAsMaybe(Mono<T> source) {
            this.source = source;
        }

        @Override
        protected void subscribeActual(MaybeObserver<? super T> observer) {
            source.subscribe(new MonoSubscriber<T>(observer));
        }

        static final class MonoSubscriber<T> implements CoreSubscriber<T>, io.reactivex.rxjava3.disposables.Disposable {
            final MaybeObserver<? super T> actual;

            Subscription s;

            public MonoSubscriber(MaybeObserver<? super T> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Subscription s) {
                this.s = s;

                actual.onSubscribe(this);

                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T t) {
                s = Operators.cancelledSubscription();
                actual.onSuccess(t);
            }

            @Override
            public void onError(Throwable t) {
                s = Operators.cancelledSubscription();
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                if (s != Operators.cancelledSubscription()) {
                    s = Operators.cancelledSubscription();
                    actual.onComplete();
                }
            }

            @Override
            public void dispose() {
                s.cancel();
                s = Operators.cancelledSubscription();
            }

            @Override
            public boolean isDisposed() {
                return s == Operators.cancelledSubscription();
            }
        }
    }

    static final class MaybeAsMono<T> extends Mono<T> implements Fuseable {
        final Maybe<T> source;

        public MaybeAsMono(Maybe<T> source) {
            this.source = source;
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> s) {
            source.subscribe(new MaybeAsMonoObserver<>(s));
        }

        static final class MaybeAsMonoObserver<T> extends MonoSubscriber<T, T> implements MaybeObserver<T> {

            io.reactivex.rxjava3.disposables.Disposable d;

            public MaybeAsMonoObserver(CoreSubscriber<? super T> subscriber) {
                super(subscriber);
            }

            @Override
            public void onSubscribe(io.reactivex.rxjava3.disposables.Disposable d) {
                this.d = d;

                actual.onSubscribe(this);
            }

            @Override
            public void onSuccess(T value) {
                complete(value);
            }

            @Override
            public void cancel() {
                super.cancel();
                d.dispose();
            }
        }
    }

    /**
     * A variation of {@link io.reactivex.rxjava3.internal.operators.completable.CompletableFromPublisher},
     * but with the option to process discarded elements from the {@link reactor.core.publisher.Flux}
     * or {@link reactor.core.publisher.Mono} source. Since the downstream doesn't define a {@link reactor.util.context.Context},
     * this handler is passed explicitly.
     *
     * @author Simon Baslé
     */
    static final class Completable3FromCorePublisher<T> extends Completable {

        final Publisher<T> source;
        final Consumer<T> optionalDiscardHandler;

        Completable3FromCorePublisher(Publisher<T> source, Consumer<T> discardHandler) {
            this.source = source;
            this.optionalDiscardHandler = discardHandler;
        }

        @Override
        protected void subscribeActual(final CompletableObserver downstream) {
            source.subscribe(new FromCorePublisherSubscriber<>(downstream, optionalDiscardHandler));
        }

        static final class FromCorePublisherSubscriber<T> implements FlowableSubscriber<T>,
                                                                     io.reactivex.rxjava3.disposables.Disposable {

            final CompletableObserver downstream;
            final Consumer<T> optionalDiscardHandler;

            Subscription upstream;

            /**
             * @param downstream the {@link CompletableObserver} to notify
             * @param optionalDiscardHandler a handler for values that get discarded, MUST complete normally (no exceptions), or null if not relevant
             */
            FromCorePublisherSubscriber(CompletableObserver downstream, Consumer<T> optionalDiscardHandler) {
                this.downstream = downstream;
                this.optionalDiscardHandler = optionalDiscardHandler;
            }

            @Override
            public void onSubscribe(Subscription s) {
                if (SubscriptionHelper.validate(this.upstream, s)) {
                    this.upstream = s;

                    downstream.onSubscribe(this);

                    s.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(T t) {
                optionalDiscardHandler.accept(t);
                //ignore the discarded value
            }

            @Override
            public void onError(Throwable t) {
                downstream.onError(t);
            }

            @Override
            public void onComplete() {
                downstream.onComplete();
            }

            @Override
            public void dispose() {
                upstream.cancel();
                upstream = SubscriptionHelper.CANCELLED;
            }

            @Override
            public boolean isDisposed() {
                return upstream == SubscriptionHelper.CANCELLED;
            }
        }
    }

    static final class DisposableFromRxJava3Disposable implements Disposable {

        final io.reactivex.rxjava3.disposables.Disposable delegate;

        DisposableFromRxJava3Disposable(io.reactivex.rxjava3.disposables.Disposable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void dispose() {
            delegate.dispose();
        }

        @Override
        public boolean isDisposed() {
            return delegate.isDisposed();
        }
    }

    /** Utility class. */
    RxJava3Adapter() {}
}
