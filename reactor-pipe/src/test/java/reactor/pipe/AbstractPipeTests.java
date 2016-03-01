package reactor.pipe;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;
import org.pcollections.TreePVector;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.router.NoOpRouter;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public abstract class AbstractPipeTests extends AbstractRawBusTests {

    protected abstract <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> value);

    @Test
    public void testMap() throws InterruptedException {
        AVar<String> res = new AVar<>();

        subscribeAndDispatch(
            integerPipe
                .map(i -> {
                    return i + 1;
                })
                .map(i -> i * 2)
                .map(Object::toString)
                .consume(res::set),
            Arrays.asList(1));

        assertThat(res.get(1, TimeUnit.SECONDS), is("4"));
    }


    @Test
    public void testConsumeWithSupplier() throws InterruptedException {
        AVar<Integer> res = new AVar<>();

        subscribeAndDispatch(
            integerPipe
                .map(i -> i + 1)
                .map(i -> i * 2)
                .consume(() -> (k, v) -> res.set(v)),
            Arrays.asList(1));

        assertThat(res.get(1, TimeUnit.SECONDS), is(4));
    }



    @Test
    public void testStatefulMap() throws InterruptedException {
        AVar<Integer> res = new AVar<>(3);

        subscribeAndDispatch(
            integerPipe.map((i) -> i + 1)
                .map((Atom<Integer> state, Integer i) -> {
                         return state.update(old -> old + i);
                     },
                     0)
                .consume(res::set),
            Arrays.asList(1, 2, 3));

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
    }

    @Test
    public void scanTest() throws InterruptedException {
        AVar<Integer> res = new AVar<>(3);

        subscribeAndDispatch(
            integerPipe.scan((acc, i) -> acc + i,
                                       0)
                .consume(res::set),
            Arrays.asList(1, 2, 3));

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(6));
    }

    @Test
    public void testFilter() throws InterruptedException {
        AVar<Integer> res = new AVar<>();

        subscribeAndDispatch(
            integerPipe
                .map(i -> i + 1)
                .filter(i -> i % 2 != 0)
                .map(i -> i * 2)
                .consume(res::set),
            Arrays.asList(1, 2));

        assertThat(res.get(1, TimeUnit.SECONDS), is(6));
    }

    @Test
    public void testPartition() throws InterruptedException {
        AVar<List<Integer>> res = new AVar<>();

        subscribeAndDispatch(
            integerPipe
                .partition((i) -> {
                    return i.size() == 5;
                })
                .consume(res::set),
            Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(1, 2, 3, 4, 5))));
    }

    @Test
    public void testSlide() throws InterruptedException {
        AVar<List<Integer>> res = new AVar<>(6);

        subscribeAndDispatch(
            integerPipe
                .slide(i -> i.subList(i.size() > 5 ? i.size() - 5 : 0,
                                      i.size()))
                .consume(res::set),
            Arrays.asList(1, 2, 3, 4, 5, 6));

        assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(2, 3, 4, 5, 6))));
    }

    @Test
    public void testConsume() throws InterruptedException {
        AVar<Integer> resValue = new AVar<>();
        AVar<Key> resKey = new AVar<>();

        subscribeAndDispatch(
            integerPipe
                .map((i) -> i + 1)
                .map(i -> i * 2)
                .consume((k, v) -> {
                    resKey.set(k);
                    resValue.set(v);
                }),
            Arrays.asList(1));

        assertThat(resKey.get(1, TimeUnit.SECONDS).getPart(0), is("source"));
        assertThat(resValue.get(1, TimeUnit.SECONDS), is(4));
    }

    @Test
    public void testThrottle() throws InterruptedException {
        // With Throttle, we'd like to verify that consequent calls
        // forced the scheduling of the execution a `timeout value`
        // after the __first__ call.
        AVar<Integer> res = new AVar<>(1);

        AtomicLong end = new AtomicLong();
        subscribeAndDispatch(
            integerPipe.throttle(1, TimeUnit.SECONDS)
                .consume((v) -> {
                    res.set(v);
                    end.set(System.nanoTime());
                }),
            Arrays.asList(1, 2));

        long start = System.nanoTime();
        for (int i = 0; i <= 100; i++) {
            firehose.notify(Key.wrap("source", "first"),
                            i);
            Thread.sleep(10);
        }

        // ensure that the last event processed is "100"
        assertTrue(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT) < 100);
        // The end time should be close to 1 second after the first.
        // It should be exactly 1, but to timer inaccuracies >1, never <1.
        assertTrue(TimeUnit.SECONDS.convert(end.get() - start, NANOSECONDS) >= 1);
    }

    @Test
    public void testThrottleFireFirst() throws InterruptedException {
        AVar<Integer> res = new AVar<>(1);

        subscribeAndDispatch(
            integerPipe.throttle(1, TimeUnit.SECONDS, true)
                .consume((v) -> {
                    res.set(v);
                }),
            Arrays.asList(500, 2));

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(500));
    }

    @Test
    public void testDebounce() throws InterruptedException {
        // With Debounce, we'd like to verify that consequent calls
        // forced scheduling of the execution a `timeout value` after
        // the __last__ call.
        AVar<Integer> res = new AVar<>(1);

        AtomicLong end = new AtomicLong();
        subscribeAndDispatch(
            integerPipe.debounce(1, TimeUnit.SECONDS)
                .consume((v) -> {
                    res.set(v);
                    end.set(System.nanoTime());
                }),
            Collections.emptyList());

        long start = System.nanoTime();
        for (int i = 0; i <= 100; i++) {
            firehose.notify(Key.wrap("source", "first"),
                            i);
            Thread.sleep(10);
        }

        // ensure that the last event processed is "100"
        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(100));
        // The end time should be close to 1 second after sending the last item "100".
        // It should be exactly 1, but to timer inaccuracies >1, never <1.
        assertTrue(TimeUnit.SECONDS.convert(end.get() - start, NANOSECONDS) >= 1);
    }

    @Test
    public void testDebounceFirst() throws InterruptedException {
        AVar<Integer> res = new AVar<>(1);

        subscribeAndDispatch(
            integerPipe.debounce(1, TimeUnit.SECONDS, true)
                .consume((v) -> {
                    res.set(v);
                }),
            Arrays.asList(500, 2));

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(500));
    }


    @Test
    public void tailRecRedispatchTest() throws InterruptedException {
        WorkQueueProcessor<Runnable> processor = WorkQueueProcessor.<Runnable>create(
            Executors.newFixedThreadPool(1),
            2);
        RawBus<Key, Object> bus = new RawBus<Key, Object>(new ConcurrentRegistry<>(),
                                                          processor,
                                                          1,
                                                          new NoOpRouter<>(),
                                                          null,
                                                          null);

        final int iterations = 200;
        CountDownLatch latch = new CountDownLatch(iterations);
        Key k = Key.wrap("source", "first");
        integerPipe.map((i) -> i - 1)
            .consume((i) ->{
                LockSupport.parkNanos(10000);
                if (i > 0) {
                    bus.notify(k, i);
                }
                latch.countDown();
            })
            .subscribe(k, bus);

        bus.notify(k, iterations);

        latch.await(10, TimeUnit.SECONDS);
        assertThat(latch.getCount(), is(0L));
    }

    @Test
    public void testSmoke() throws InterruptedException { // Tests backpressure and in-thread dispatches
        WorkQueueProcessor<Runnable> processor = WorkQueueProcessor.<Runnable>create(
                Executors.newFixedThreadPool(4),
                256);
        RawBus<Key, Object> bus = new RawBus<Key, Object>(new ConcurrentRegistry<>(),
                processor,
                4,
                new NoOpRouter<>(),
                null,
                null);


        int iterations = 2000;
        CountDownLatch latch = new CountDownLatch(iterations);

        integerPipe
                .map((i) -> i + 1)
                .map(i -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return i * 2;
                })
                .consume((i_) -> {
                    latch.countDown();
                }).subscribe(Key.wrap("source", "first"), bus);

        for (int i = 0; i < iterations; i++) {
            bus.notify(Key.wrap("source", "first"), i);
            if (i % 500 == 0) {
                System.out.println("Processed " + i + " keys");
            }
        }

        latch.await(5, TimeUnit.MINUTES);
        assertThat(latch.getCount(), is(0L));

        processor.shutdown();
    }
}
