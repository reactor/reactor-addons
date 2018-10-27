/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.extra;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class SwitchTransformOnFirstFlux<T, R> extends Flux<R> {

    final Publisher<? extends T> source;
    final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;

    public SwitchTransformOnFirstFlux(
            Publisher<? extends T> source, BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
        this.source = Objects.requireNonNull(source, "source");
        this.transformer = Objects.requireNonNull(transformer, "transformer");
    }

    @Override
    public int getPrefetch() {
        return 1;
    }

    @Override
    public void subscribe(CoreSubscriber<? super R> actual) {
        source.subscribe(new SwitchTransformMain<>(actual, transformer));
    }

    static final class SwitchTransformMain<T, R> implements CoreSubscriber<T>, Scannable {

        final CoreSubscriber<? super R> actual;
        final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;

        Subscription            s;
        SwitchTransformInner<T> inner;

        SwitchTransformMain(
                CoreSubscriber<? super R> actual,
                BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
            this.actual = actual;
            this.transformer = transformer;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
            if (key == Attr.PREFETCH) return 1;

            return null;
        }

        @Override
        public Context currentContext() {
            SwitchTransformInner<T> i = inner;

            if (i != null) {
                CoreSubscriber<? super T> actual = i.actual;

                if (actual != null) {
                    return actual.currentContext();
                }
            }

            return actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                s.request(1);
            }
        }

        @Override
        public void onNext(T t) {
            if (s == null) {
                Operators.onNextDropped(t, currentContext());
                return;
            }

            SwitchTransformInner<T> i = inner;

            if (i == null) {
                i = inner = new SwitchTransformInner<>(this);

                try {
                    i.first = t;
                    Publisher<? extends R> result =
                            Objects.requireNonNull(
                                    transformer.apply(t, i), "The transformer returned a null value");
                    result.subscribe(actual);
                    return;
                }
                catch (Throwable e) {
                    onError(Operators.onOperatorError(s, e, t, currentContext()));
                    return;
                }
            }

            inner.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (s == null) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }

            SwitchTransformInner<T> i = inner;

            if (i != null) {
                i.onError(t);
            }
            else {
                s = null;
                Operators.error(actual, t);
            }
        }

        @Override
        public void onComplete() {
            SwitchTransformInner<T> i = inner;

            if (i != null) {
                i.onComplete();
            }
            else {
                s = null;
                Operators.complete(actual);
            }
        }

        void cancel() {
            Subscription s = this.s;
            if (s != Operators.cancelledSubscription()) {
                this.s = Operators.cancelledSubscription();
                s.cancel();
            }
        }

        void request(long n) {
            s.request(n);
        }
    }

    static final class SwitchTransformInner<V> extends Flux<V> implements Scannable, Subscription {


        final SwitchTransformMain<V, ?> parent;

        boolean   done;
        Throwable throwable;

        volatile V first;

        volatile CoreSubscriber<? super V> actual;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<SwitchTransformInner, CoreSubscriber> ACTUAL =
                AtomicReferenceFieldUpdater.newUpdater(
                        SwitchTransformInner.class, CoreSubscriber.class, "actual");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchTransformInner> WIP =
                AtomicIntegerFieldUpdater.newUpdater(SwitchTransformInner.class, "wip");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchTransformInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(SwitchTransformInner.class, "once");

        SwitchTransformInner(SwitchTransformMain<V, ?> parent) {
            this.parent = parent;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return parent;
            if (key == Attr.ACTUAL) return actual;

            return null;
        }

        public CoreSubscriber<? super V> actual() {
            return actual;
        }

        @Override
        public void subscribe(CoreSubscriber<? super V> actual) {
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                ACTUAL.lazySet(this, actual);
                actual.onSubscribe(this);
            }
            else {
                actual.onSubscribe(Operators.emptySubscription());
                actual.onError(new IllegalStateException("SwitchTransform allows only one Subscriber"));
            }
        }

        @Override
        public void request(long n) {
            if (first != null && drainRegular() && n != Long.MAX_VALUE) {
                n = Operators.addCap(n, -1);
                if (n > 0) {
                    parent.request(n);
                }
            }
            else {
                parent.request(n);
            }
        }

        @Override
        public void cancel() {
            if (actual != null) {
                if (WIP.getAndIncrement(this) == 0) {
                    actual = null;
                    first = null;
                }

                parent.cancel();
            }
        }

        boolean drainRegular() {
            if (WIP.getAndIncrement(this) != 0) {
                return false;
            }

            V f = first;
            int m = 1;
            boolean sent = false;
            Subscription s = parent.s;
            CoreSubscriber<? super V> a = actual;

            for (;;) {
                if (f != null) {
                    first = null;

                    if (s == Operators.cancelledSubscription()) {
                        Operators.onNextDropped(f, a.currentContext());
                        return true;
                    }

                    a.onNext(f);
                    f = null;
                    sent = true;
                }

                if (s == Operators.cancelledSubscription()) {
                    return sent;
                }

                if (done) {
                    Throwable t = throwable;
                    if (t != null) {
                        a.onError(t);
                    }
                    else {
                        a.onComplete();
                    }
                    return sent;
                }


                m = WIP.addAndGet(this, -m);

                if (m == 0) {
                    return sent;
                }
            }
        }

        void onNext(V t) {
            if (done) {
                Operators.onNextDropped(t, currentContext());
                return;
            }

            CoreSubscriber<? super V> a = actual;

            if (a != null) {
                a.onNext(t);
            }
        }

        void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }

            throwable = t;
            done = true;

            if (first == null) {
                drainRegular();
            }
        }

        void onComplete() {
            done = true;

            if (first == null) {
                drainRegular();
            }
        }

        Context currentContext() {
            CoreSubscriber<? super V> actual = this.actual;
            return actual != null ? actual.currentContext() : Context.empty();
        }
    }
}
