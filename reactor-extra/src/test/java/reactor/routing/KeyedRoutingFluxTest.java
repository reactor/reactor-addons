package reactor.routing;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.QueueSupplier;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KeyedRoutingFluxTest {
    
    @Test
    public void supportKeyedRouting() {
        KeyedRoutingFlux<Integer, Integer> routingFlux = KeyedRoutingFlux.create(Flux.range(1, 5),
                QueueSupplier.SMALL_BUFFER_SIZE, QueueSupplier.get(QueueSupplier.SMALL_BUFFER_SIZE),
                value -> value % 2);

        Flux<Integer> evenFlux = routingFlux.route(0);
        Flux<Integer> oddFlux = routingFlux.route(1);

        Mono<List<Integer>> evenListMono = evenFlux.collectList().subscribe();
        Mono<List<Integer>> oddListMono = oddFlux.collectList().subscribe();

        routingFlux.connect();

        assertEquals(Arrays.asList(2, 4), evenListMono.block());
        assertEquals(Arrays.asList(1, 3, 5), oddListMono.block());
    }
}
