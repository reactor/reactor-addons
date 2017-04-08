package reactor.routing;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.concurrent.QueueSupplier;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class PredicateRoutingFluxTest {

    @Test
    public void supportFluentRoutingSyntax() {
        PredicateRoutingFlux<Integer, Integer> routingFlux = PredicateRoutingFlux.create(Flux.range(1, 5),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                Function.identity());

        Flux<Integer> evenFlux = routingFlux.route(x -> x % 2 == 0);
        Flux<Integer> oddFlux = routingFlux.route(x -> x % 2 != 0);

        Mono<List<Integer>> evenListMono = evenFlux.collectList().subscribe();
        Mono<List<Integer>> oddListMono = oddFlux.collectList().subscribe();

        assertEquals(Arrays.asList(2, 4), evenListMono.block());
        assertEquals(Arrays.asList(1, 3, 5), oddListMono.block());
    }

    @Test
    public void fluentRoutingSubscribePartiallyLastUnconsumed() {
        PredicateRoutingFlux<Integer, Integer> routingFlux = PredicateRoutingFlux.create(Flux.range(1, 5),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                Function.identity());

        Flux<Integer> evenFlux = routingFlux.route(x -> x % 2 == 0);
        routingFlux.route(x -> x % 2 != 0).subscribe();

        MonoProcessor<List<Integer>> evenListMono = evenFlux.collectList().subscribe();

        assertEquals(Arrays.asList(2, 4), evenListMono.block());
    }

    @Test
    public void fluentRoutingSubscribePartiallyLastConsumed() {
        PredicateRoutingFlux<Integer, Integer> routingFlux = PredicateRoutingFlux.create(Flux.range(1, 5),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                Function.identity());

        routingFlux.route(x -> x % 2 == 0).log().subscribe();
        Flux<Integer> oddFlux = routingFlux.route(x -> x % 2 != 0).log();

        Mono<List<Integer>> oddListMono = oddFlux.collectList().subscribe();

        assertEquals(Arrays.asList(1, 3, 5), oddListMono.block());
    }

}
