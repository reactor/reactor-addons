package reactor.routing;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.QueueSupplier;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class KeyedRoutingFluxTest {
    
    @Test
    public void supportKeyedRouting() {
        KeyedRoutingFlux<Integer, Integer> routingFlux = KeyedRoutingFlux.create(Flux.range(1, 5),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                value -> value % 2, true);

        Flux<Integer> evenFlux = routingFlux.route(0);
        Flux<Integer> oddFlux = routingFlux.route(1);

        Mono<List<Integer>> evenListMono = evenFlux.collectList().subscribe();
        Mono<List<Integer>> oddListMono = oddFlux.collectList().subscribe();

        assertEquals(Arrays.asList(2, 4), evenListMono.block());
        assertEquals(Arrays.asList(1, 3, 5), oddListMono.block());
    }

    @Test
    public void supportFluentKeyedRouting() {
        KeyedRoutingFlux<Integer, Integer> routingFlux = KeyedRoutingFlux.create(Flux.range(1, 5),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                value -> value % 2, true);

        AtomicReference<Mono<List<Integer>>> evenListMono = new AtomicReference<>();
        AtomicReference<Mono<List<Integer>>> oddListMono = new AtomicReference<>();
        routingFlux.route(0, flux -> evenListMono.set(flux.collectList().subscribe()))
                .route(1, flux -> oddListMono.set(flux.collectList().subscribe()));

        assertEquals(Arrays.asList(2, 4), evenListMono.get().block());
        assertEquals(Arrays.asList(1, 3, 5), oddListMono.get().block());
    }

    @Test
    public void supportOtherwiseRouting() {
        KeyedRoutingFlux<Integer, Integer> routingFlux = KeyedRoutingFlux.create(Flux.range(1, 10),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                value -> value % 3, true);

        Flux<Integer> remainderZero = routingFlux.route(0);
        Flux<Integer> remainderOther = routingFlux.routeOtherwise();

        Mono<List<Integer>> remainderZeroMono = remainderZero.collectList().subscribe();
        Mono<List<Integer>> remainderOtherMono = remainderOther.collectList().subscribe();

        assertEquals(Arrays.asList(3, 6, 9), remainderZeroMono.block());
        assertEquals(Arrays.asList(1, 2, 4, 5, 7, 8, 10), remainderOtherMono.block());

    }

}
