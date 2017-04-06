package reactor.routing;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.QueueSupplier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * One-to-many routing flux
 * <p>
 * Has one source and multiple sinks
 *
 * @param <T>
 */
public class RoutingFlux<T,K> extends ConnectableFlux<T> implements Scannable {

    public static <T> RoutingFlux<T,T> create(Flux<T> source) {
        return create(source, QueueSupplier.SMALL_BUFFER_SIZE);
    }

    public static <T> RoutingFlux<T,T> create(Flux<T> source, int prefetch) {
        return create(source, prefetch, Function.identity(), (subscribers, k) -> subscribers);
    }

    public static <T,K> RoutingFlux<T,K> create(Flux<T> source, int prefetch, Function<T, K> keyFunction,
                                                BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>> subscriberFilter) {
        return create(source, prefetch, keyFunction, subscriberFilter,
                (subscriber) -> {}, (subscriber) -> {});
    }

    public static <T,K> RoutingFlux<T,K> create(Flux<T> source, int prefetch, Function<? super T, K> keyFunction,
                                                BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>>
                                                        subscriptionFilter,
                                                Consumer<Subscriber<? super T>> onSubscription,
                                                Consumer<Subscriber<? super T>> onRemoval) {
        return (RoutingFlux<T,K> ) onAssembly(new RoutingFlux<>(source, prefetch, QueueSupplier
                .get(prefetch), keyFunction, subscriptionFilter, onSubscription, onRemoval));
    }

    /**
     * The source observable.
     */
    final Flux<? extends T> source;

    final Function<? super T, K> routingKeyFunction;

    final BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>> subscriberFilter;

    final Consumer<Subscriber<? super T>> onSubscriberAdded;

    final Consumer<Subscriber<? super T>> onSubscriberRemoved;

    /**
     * The size of the prefetch buffer.
     */
    final int prefetch;

    final Supplier<? extends Queue<T>> queueSupplier;

    volatile RoutingFlux.PublishSubscriber<T,K> connection;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<RoutingFlux, PublishSubscriber> CONNECTION =
            AtomicReferenceFieldUpdater.newUpdater(RoutingFlux.class,
                    RoutingFlux.PublishSubscriber.class,
                    "connection");

    RoutingFlux(Flux<? extends T> source,
                int prefetch,
                Supplier<? extends Queue<T>> queueSupplier, Function<? super T, K> routingKeyFunction, BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>> subscriberFilter, Consumer<Subscriber<? super T>> onSubscriberAdded, Consumer<Subscriber<? super T>> onSubscriberRemoved) {
        this.routingKeyFunction = routingKeyFunction;
        this.subscriberFilter = subscriberFilter;
        this.onSubscriberAdded = onSubscriberAdded;
        this.onSubscriberRemoved = onSubscriberRemoved;
        if (prefetch <= 0) {
            throw new IllegalArgumentException("bufferSize > 0 required but it was " + prefetch);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.prefetch = prefetch;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }

    @Override
    public void connect(Consumer<? super Disposable> cancelSupport) {
        boolean doConnect;
        RoutingFlux.PublishSubscriber<T,K> s;
        for (; ; ) {
            s = connection;
            if (s == null || s.isTerminated()) {
                RoutingFlux.PublishSubscriber<T,K> u = new RoutingFlux.PublishSubscriber<>(prefetch, this);

                if (!CONNECTION.compareAndSet(this, s, u)) {
                    continue;
                }

                s = u;
            }

            doConnect = s.tryConnect();
            break;
        }

        cancelSupport.accept(s);
        if (doConnect) {
            source.subscribe(s);
        }
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        RoutingFlux.PublishInner<T,K> inner = new RoutingFlux.PublishInner<>(s);
        s.onSubscribe(inner);
        for (; ; ) {
            if (inner.isCancelled()) {
                break;
            }

            RoutingFlux.PublishSubscriber<T,K> c = connection;
            if (c == null || c.isTerminated()) {
                RoutingFlux.PublishSubscriber<T,K> u = new RoutingFlux.PublishSubscriber<>(prefetch, this);
                if (!CONNECTION.compareAndSet(this, c, u)) {
                    continue;
                }

                c = u;
            }

            if (c.trySubscribe(inner)) {
                break;
            }
        }
    }

    @Override
    public int getPrefetch() {
        return prefetch;
    }

    @Override
    public Object scan(Scannable.Attr key) {
        switch (key) {
            case PREFETCH:
                return getPrefetch();
            case PARENT:
                return source;
        }
        return null;
    }

    static final class PublishSubscriber<T,K>
            implements Subscriber<T>, Scannable, Disposable {

        final int prefetch;

        final RoutingFlux<T,K> parent;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublishSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(RoutingFlux.PublishSubscriber.class,
                        Subscription.class,
                        "s");

        volatile RoutingFlux.PublishInner<T,K> [] subscribers;
        volatile Map<Subscriber<? super T>, PublishInner<T,K>> actualSubscriberToInner;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublishSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(RoutingFlux.PublishSubscriber.class, "wip");

        volatile int connected;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublishSubscriber> CONNECTED =
                AtomicIntegerFieldUpdater.newUpdater(RoutingFlux.PublishSubscriber.class,
                        "connected");

        @SuppressWarnings("rawtypes")
        static final RoutingFlux.PublishInner[] EMPTY = new RoutingFlux.PublishInner[0];
        @SuppressWarnings("rawtypes")
        static final RoutingFlux.PublishInner[] TERMINATED = new RoutingFlux.PublishInner[0];

        volatile Queue<T> queue;

        int sourceMode;

        volatile boolean done;
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublishSubscriber, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(RoutingFlux.PublishSubscriber.class,
                        Throwable.class,
                        "error");

        volatile boolean cancelled;

        @SuppressWarnings("unchecked")
        PublishSubscriber(int prefetch, RoutingFlux<T,K> parent) {
            this.prefetch = prefetch;
            this.parent = parent;
            this.subscribers = EMPTY;
        }

        boolean isTerminated() {
            return subscribers == TERMINATED;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.setOnce(S, this, s)) {
                if (s instanceof Fuseable.QueueSubscription) {
                    @SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> f =
                            (Fuseable.QueueSubscription<T>) s;

                    int m = f.requestFusion(Fuseable.ANY);
                    if (m == Fuseable.SYNC) {
                        sourceMode = m;
                        queue = f;
                        done = true;
                        drain();
                        return;
                    } else if (m == Fuseable.ASYNC) {
                        sourceMode = m;
                        queue = f;
                        s.request(prefetch);
                        return;
                    }
                }

                queue = parent.queueSupplier.get();

                s.request(prefetch);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                if (t != null) {
                    Operators.onNextDropped(t);
                }
                return;
            }
            if (sourceMode == Fuseable.ASYNC) {
                drain();
                return;
            }

            if (!queue.offer(t)) {
                Throwable ex = Operators.onOperatorError(s,
                        Exceptions.failWithOverflow("Queue is full?!"),
                        t);
                if (!Exceptions.addThrowable(ERROR, this, ex)) {
                    Operators.onErrorDropped(ex);
                    return;
                }
                done = true;
            }
            drain();
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t);
                return;
            }
            if (Exceptions.addThrowable(ERROR, this, t)) {
                done = true;
                drain();
            } else {
                Operators.onErrorDropped(t);
            }
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        @Override
        public void dispose() {
            if (cancelled) {
                return;
            }
            if (Operators.terminate(S, this)) {
                cancelled = true;
                if (WIP.getAndIncrement(this) != 0) {
                    return;
                }
                disconnectAction();
            }
        }

        void disconnectAction() {
            queue.clear();
            CancellationException ex = new CancellationException("Disconnected");
            for (RoutingFlux.PublishInner<T,K> inner : terminate()) {
                inner.actual.onError(ex);
            }
        }

        boolean add(RoutingFlux.PublishInner<T,K> inner) {
            if (subscribers == TERMINATED) {
                return false;
            }
            synchronized (this) {
                RoutingFlux.PublishInner<T,K> [] a = subscribers;
                if (a == TERMINATED) {
                    return false;
                }
                int n = a.length;

                @SuppressWarnings("unchecked") RoutingFlux.PublishInner<T,K> [] b =
                        new RoutingFlux.PublishInner[n + 1];
                System.arraycopy(a, 0, b, 0, n);
                b[n] = inner;

                subscribers = b;
                if (actualSubscriberToInner == null) {
                    actualSubscriberToInner = new HashMap<>();
                }
                actualSubscriberToInner.put(inner.actual, inner);
                parent.onSubscriberAdded.accept(inner.actual);
                return true;
            }
        }

        @SuppressWarnings("unchecked")
        void remove(RoutingFlux.PublishInner<T,K> inner) {
            RoutingFlux.PublishInner<T,K> [] a = subscribers;
            if (a == TERMINATED || a == EMPTY) {
                return;
            }
            synchronized (this) {
                a = subscribers;
                if (a == TERMINATED || a == EMPTY) {
                    return;
                }

                int j = -1;
                int n = a.length;
                for (int i = 0; i < n; i++) {
                    if (a[i] == inner) {
                        j = i;
                        break;
                    }
                }
                if (j < 0) {
                    return;
                }

                RoutingFlux.PublishInner<T,K> [] b;
                if (n == 1) {
                    b = EMPTY;
                } else {
                    b = new RoutingFlux.PublishInner[n - 1];
                    System.arraycopy(a, 0, b, 0, j);
                    System.arraycopy(a, j + 1, b, j, n - j - 1);
                }

                subscribers = b;
                actualSubscriberToInner.remove(inner.actual);
                parent.onSubscriberRemoved.accept(inner.actual);
            }
        }

        @SuppressWarnings("unchecked")
        RoutingFlux.PublishInner<T,K> [] terminate() {
            RoutingFlux.PublishInner<T,K> [] a;
            for (; ; ) {
                a = subscribers;
                if (a == TERMINATED) {
                    return a;
                }
                synchronized (this) {
                    a = subscribers;
                    if (a != TERMINATED) {
                        subscribers = TERMINATED;
                        actualSubscriberToInner = null;
                    }
                    return a;
                }
            }
        }

        boolean tryConnect() {
            return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
        }

        boolean trySubscribe(RoutingFlux.PublishInner<T,K> inner) {
            if (add(inner)) {
                if (inner.isCancelled()) {
                    remove(inner);
                } else {
                    inner.parent = this;
                    drain();
                }
                return true;
            }
            return false;
        }

        void replenish(long n) {
            if (sourceMode != Fuseable.SYNC) {
                s.request(n);
            }
        }

        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;

            for (; ; ) {

                boolean d = done;

                Queue<T> q = queue;

                boolean empty = q == null || q.isEmpty();

                if (checkTerminated(d, empty)) {
                    return;
                }

                if (!empty) {
                    RoutingFlux.PublishInner<T,K> [] a = subscribers;
                    long maxRequested = Long.MAX_VALUE;

                    int len = a.length;
                    int cancel = 0;

                    for (RoutingFlux.PublishInner<T,K> inner : a) {
                        long r = inner.requested;
                        if (r >= 0L) {
                            maxRequested = Math.min(maxRequested, r);
                        } else { //Long.MIN == PublishInner.CANCEL_REQUEST
                            cancel++;
                        }
                    }

                    if (len == cancel) {
                        T v;

                        try {
                            v = q.poll();
                        } catch (Throwable ex) {
                            Exceptions.addThrowable(ERROR,
                                    this,
                                    Operators.onOperatorError(s, ex));
                            d = true;
                            v = null;
                        }
                        if (checkTerminated(d, v == null)) {
                            return;
                        }
                        replenish(1);
                        continue;
                    }

                    int e = 0;

                    while (e < maxRequested && cancel != Integer.MIN_VALUE) {
                        d = done;
                        T v;

                        try {
                            v = q.poll();
                        } catch (Throwable ex) {
                            Exceptions.addThrowable(ERROR,
                                    this,
                                    Operators.onOperatorError(s, ex));
                            d = true;
                            v = null;
                        }

                        empty = v == null;

                        if (checkTerminated(d, empty)) {
                            return;
                        }

                        if (empty) {
                            break;
                        }

                        K key = parent.routingKeyFunction.apply(v);

                        Stream<Subscriber<? super T>> filtered = parent.subscriberFilter.apply(Arrays.stream(a).map(x
                                -> x.actual), key);

                        for(Subscriber<? super T> actualSubscriber : (Iterable<Subscriber<? super T>>)filtered::iterator) {
                            RoutingFlux.PublishInner<T, K> inner =
                                    actualSubscriberToInner.get(actualSubscriber);
                            if (inner != null) {
                                inner.actual.onNext(v);
                                if (inner.produced(1) == RoutingFlux.PublishInner.CANCEL_REQUEST) {
                                    cancel = Integer.MIN_VALUE;
                                }
                            }
                        }

                        e++;
                    }

                    if (e != 0) {
                        replenish(e);
                    }

                    if (maxRequested != 0L && !empty) {
                        continue;
                    }
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        boolean checkTerminated(boolean d, boolean empty) {
            if (cancelled) {
                disconnectAction();
                return true;
            }
            if (d) {
                Throwable e = error;
                if (e != null && e != Exceptions.TERMINATED) {
                    CONNECTION.compareAndSet(parent, this, null);
                    e = Exceptions.terminate(ERROR, this);
                    queue.clear();
                    for (RoutingFlux.PublishInner<T,K> inner : terminate()) {
                        inner.actual.onError(e);
                    }
                    return true;
                } else if (empty) {
                    CONNECTION.compareAndSet(parent, this, null);
                    for (RoutingFlux.PublishInner<T,K> inner : terminate()) {
                        inner.actual.onComplete();
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        public Stream<? extends Scannable> inners() {
            return Stream.of(subscribers);
        }

        @Override
        public Object scan(Attr key) {
            switch (key) {
                case PARENT:
                    return s;
                case PREFETCH:
                    return prefetch;
                case ERROR:
                    return error;
                case BUFFERED:
                    return queue != null ? queue.size() : 0;
                case TERMINATED:
                    return isTerminated();
                case CANCELLED:
                    return cancelled;
            }
            return null;
        }

        @Override
        public boolean isDisposed() {
            return cancelled || done;
        }

    }

    static final class PublishInner<T,K> implements Scannable, Subscription {

        final Subscriber<? super T> actual;

        RoutingFlux.PublishSubscriber<T,K> parent;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublishInner> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(RoutingFlux.PublishInner.class, "requested");

        PublishInner(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                requested(this, n);
                RoutingFlux.PublishSubscriber<T,K> p = parent;
                if (p != null) {
                    p.drain();
                }
            }
        }

        static final long CANCEL_REQUEST = Long.MIN_VALUE;

        @Override
        public void cancel() {
            long r = requested;
            if (r != Long.MIN_VALUE) {
                r = REQUESTED.getAndSet(this, CANCEL_REQUEST);
                if (r != CANCEL_REQUEST) {
                    RoutingFlux.PublishSubscriber<T,K> p = parent;
                    if (p != null) {
                        p.remove(this);
                        p.drain();
                    }
                }
            }
        }

        boolean isCancelled() {
            return requested == CANCEL_REQUEST;
        }

        public Subscriber<? super T> actual() {
            return actual;
        }

        @Override
        public Object scan(Attr key) {
            switch (key) {
                case PARENT:
                    return parent;
                case TERMINATED:
                    return parent != null && parent.isTerminated();
                case CANCELLED:
                    return isCancelled();
                case REQUESTED_FROM_DOWNSTREAM:
                    return isCancelled() ? 0L : requested;
            }
            if(key == Attr.ACTUAL){
                return actual();
            }
            return null;
        }

        long produced(long n) {
            return produced(this, n);
        }

        //TODO factorize in Operators ?
        static void requested(RoutingFlux.PublishInner<?,?> inner, long n) {
            for (; ; ) {
                long r = REQUESTED.get(inner);
                if (r == Long.MIN_VALUE || r == Long.MAX_VALUE) {
                    return;
                }
                long u = Operators.addCap(r, n);
                if (REQUESTED.compareAndSet(inner, r, u)) {
                    return;
                }
            }
        }

        static long produced(RoutingFlux.PublishInner<?,?> inner, long n) {
            for (; ; ) {
                long current = REQUESTED.get(inner);
                if (current == Long.MIN_VALUE) {
                    return Long.MIN_VALUE;
                }
                if (current == Long.MAX_VALUE) {
                    return Long.MAX_VALUE;
                }
                long update = current - n;
                if (update < 0L) {
                    Operators.reportBadRequest(update);
                    update = 0L;
                }
                if (REQUESTED.compareAndSet(inner, current, update)) {
                    return update;
                }
            }
        }
    }
}
