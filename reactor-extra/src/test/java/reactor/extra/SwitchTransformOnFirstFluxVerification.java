package reactor.extra;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

@Test
public class SwitchTransformOnFirstFluxVerification extends PublisherVerification<Integer> {

    public SwitchTransformOnFirstFluxVerification() {
        super(new TestEnvironment());
    }

    @Override
    public Publisher<Integer> createPublisher(long elements) {
        return Flux.range(0, Integer.MAX_VALUE < elements ? Integer.MAX_VALUE : (int) elements)
                   .transform(flux -> new SwitchTransformOnFirstFlux<>(
                           flux,
                           (first, innerFlux) -> innerFlux
                   ));
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return Flux.<Integer>error(new RuntimeException())
                   .transform(flux -> new SwitchTransformOnFirstFlux<>(
                           flux,
                           (first, innerFlux) -> innerFlux
                   ));
    }

}