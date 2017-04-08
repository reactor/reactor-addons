package reactor.routing;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class KeyedRoutingFlux<T, K> extends RoutingFlux<T, K> {
    private static class RoutingRegistry<T, K> {
        private final Map<K, List<Subscriber<? super T>>> interests = new ConcurrentHashMap<>();
        private static final List EMPTY_LIST = Collections.emptyList();

        final BiFunction<Stream<Subscriber<? super T>>, K, Stream<Subscriber<? super T>>> filter = (subscribers, k) -> {
            return interests.getOrDefault(k, EMPTY_LIST).stream();
        };

        final Consumer<Subscriber<? super T>> onSubscriberRemoved = subscriber -> {
            for(Iterator<Map.Entry<K, List<Subscriber<? super T>>>> iterator = interests.entrySet().iterator();
                iterator.hasNext(); ) {
                List<Subscriber<? super T>> subscriberList = iterator.next().getValue();
                if(subscriberList.remove(subscriber) && subscriberList.isEmpty()) {
                    iterator.remove();
                }
            }
        };

        void registerSubscriber(Subscriber<? super T> subscriber, K interestedValue) {
            List<Subscriber<? super T>> subscriberList = interests.get(interestedValue);
            if (subscriberList == null) {
                subscriberList = new CopyOnWriteArrayList<>();
                interests.put(interestedValue, subscriberList);
            }
            subscriberList.add(subscriber);
        }
    }

    public static <T, K> KeyedRoutingFlux<T, K> create(Flux<? extends T> source, int prefetch, Supplier<? extends
            Queue<T>> queueSupplier, Function<? super T, K> routingKeyFunction) {
        return new KeyedRoutingFlux<T, K>(source, prefetch, queueSupplier, routingKeyFunction,
                new RoutingRegistry<>());
    }

    private final RoutingRegistry<T, K> routingRegistry;

    KeyedRoutingFlux(Flux<? extends T> source, int prefetch, Supplier<? extends Queue<T>> queueSupplier, Function<?
            super T, K> routingKeyFunction, RoutingRegistry<T, K> routingRegistry) {
        super(source, prefetch, queueSupplier, routingKeyFunction, routingRegistry.filter,
                subscriber -> {}, routingRegistry.onSubscriberRemoved);
        this.routingRegistry = routingRegistry;
    }

    public Flux<T> route(final K interestedValue) {
        return new Flux<T>() {
            @Override
            public void subscribe(Subscriber<? super T> s) {
                routingRegistry.registerSubscriber(s, interestedValue);
                KeyedRoutingFlux.this.subscribe(s);
            }
        };
    }
}
