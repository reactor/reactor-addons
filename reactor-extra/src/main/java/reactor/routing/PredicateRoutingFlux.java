package reactor.routing;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PredicateRoutingFlux<T, K> extends RoutingFlux<T, K> {
    private static class RoutingRegistry<T, K> {
        private final Map<Subscriber<? super T>, Predicate<K>> interests = new ConcurrentHashMap<>();
        private final Set<Subscriber<? super T>> routeOtherwise = new CopyOnWriteArraySet<>();

        final BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>> filter = (subscribers, k) -> {
            return fallbackToDefault(subscribers.filter(subscriber -> interests.getOrDefault(subscriber, testVal -> false).test(k)),
                    routeOtherwise);
        };

        // Java 8 stream API doesn't have a way to peek in stream, convert to iterator and back to stream
        private static <T> Stream<T> fallbackToDefault(Stream<T> stream, Collection<T> defaultValues) {
            Iterator<T> iterator = stream.iterator();
            if (iterator.hasNext()) {
                return iteratorToStream(iterator);
            } else {
                return defaultValues.stream();
            }
        }

        private static <T> Stream<T> iteratorToStream(final Iterator<T> iterator) {
            final Iterable<T> iterable = () -> iterator;
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        final Consumer<Subscriber<? super T>> onSubscriberRemoved = subscriber -> interests.remove(subscriber);

        void registerSubscriber(Subscriber<? super T> subscriber, Predicate<K> interestFunction) {
            interests.put(subscriber, interestFunction);
        }

        public void registerSubscriberOtherwise(Subscriber<? super T> s) {
            routeOtherwise.add(s);
        }
    }

    public static <T, K> PredicateRoutingFlux<T, K> create(Flux<? extends T> source, int prefetch, Supplier<? extends
            Queue<T>> queueSupplier, Function<? super T, K> routingKeyFunction, boolean autoConnect) {
        return new PredicateRoutingFlux<T, K>(source, prefetch, queueSupplier, routingKeyFunction,
                new RoutingRegistry<>(), autoConnect);
    }

    private final RoutingRegistry<T, K> routingRegistry;

    PredicateRoutingFlux(Flux<? extends T> source, int prefetch, Supplier<? extends Queue<T>> queueSupplier, Function<?
            super T, K> routingKeyFunction, RoutingRegistry<T, K> routingRegistry, boolean autoConnect) {
        super(source, prefetch, queueSupplier, routingKeyFunction, routingRegistry.filter, autoConnect,
                subscriber -> {}, routingRegistry.onSubscriberRemoved);
        this.routingRegistry = routingRegistry;
    }

    public Flux<T> route(final Predicate<K> interest) {
        subscriberCounter.incrementAndGet();
        return new Flux<T>() {
            @Override
            public void subscribe(Subscriber<? super T> s) {
                routingRegistry.registerSubscriber(s, interest);
                PredicateRoutingFlux.this.subscribe(s);
            }
        };
    }

    public PredicateRoutingFlux<T, K> route(final Predicate<K> interest, Consumer<Flux<T>> fluxConsumer) {
        fluxConsumer.accept(route(interest));
        return this;
    }

    public Flux<T> routeOtherwise() {
        subscriberCounter.incrementAndGet();
        return new Flux<T>() {
            @Override
            public void subscribe(Subscriber<? super T> s) {
                routingRegistry.registerSubscriberOtherwise(s);
                PredicateRoutingFlux.this.subscribe(s);
            }
        };
    }

    public PredicateRoutingFlux<T, K> routeOtherwise(Consumer<Flux<T>> fluxConsumer) {
        fluxConsumer.accept(routeOtherwise());
        return this;
    }
}
