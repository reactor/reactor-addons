package reactor.pipe;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.pcollections.TreePVector;
import reactor.bus.AbstractBus;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.router.NoOpRouter;
import reactor.pipe.state.DefaultStateProvider;

public class AbstractRawBusTests {

    public static final long     LATCH_TIMEOUT   = 10;
    public static final TimeUnit LATCH_TIME_UNIT = TimeUnit.SECONDS;

    protected AbstractBus<Key, Object>          firehose;
    protected WorkQueueProcessor<Runnable> processor;
    protected Pipe<Integer, Integer> integerPipe;

    @Before
    public void setup() {
        this.processor = WorkQueueProcessor.create(Executors.newFixedThreadPool(1),
                                                                  1024);
        this.firehose = new RawBus<Key, Object>(new ConcurrentRegistry<>(),
                                                processor,
                                                1,
                                                new NoOpRouter<>(),
                                                null,
                                                null);
        this.integerPipe = new Pipe<Integer, Integer>(TreePVector.empty(), new DefaultStateProvider<Key>(), new Supplier<TimedScheduler>() {
            @Override
            public TimedScheduler get() {
                // for tests we use a higher resolution timer
                return Schedulers.newTimer("pipe-timer", 1, 512);
            }
      });
    }

    @After
    public void teardown() {
        processor.shutdown();
    }

}
