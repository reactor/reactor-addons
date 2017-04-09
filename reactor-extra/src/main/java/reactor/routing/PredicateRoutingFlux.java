package reactor.routing;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class PredicateRoutingFlux<T, K> extends RoutingFlux<T, K> {
    private static class RoutingRegistry<T, K> {
        private final Map<Subscriber<? super T>, Predicate<K>> interests = new ConcurrentHashMap<>();
        final BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>> filter = (subscribers, k) -> {
          return subscribers.filter(subscriber -> interests.getOrDefault(subscriber, testVal -> false).test(k));
        };

        final Consumer<Subscriber<? super T>> onSubscriberRemoved = subscriber -> interests.remove(subscriber);

        void registerSubscriber(Subscriber<? super T> subscriber, Predicate<K> interestFunction) {
            interests.put(subscriber, interestFunction);
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
}
