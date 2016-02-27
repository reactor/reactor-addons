package reactor.pipe.operation;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.bus.Bus;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

public class SlidingWindowOperation<SRC extends Key, DST extends Key, V> implements BiConsumer<SRC, V> {

    private final Atom<PVector<V>>       buffer;
    private final Bus<Key, Object>       firehose;
    private final UnaryOperator<List<V>> drop;
    private final DST                    destination;

    public SlidingWindowOperation(Bus<Key, Object> firehose,
                                  Atom<PVector<V>> buffer,
                                  UnaryOperator<List<V>> drop,
                                  DST destination) {
        this.buffer = buffer;
        this.firehose = firehose;
        this.drop = drop;
        this.destination = destination;
    }

    @Override
    public void accept(final SRC src, final V value) {
        PVector<V> newv = buffer.update(new UnaryOperator<PVector<V>>() {
            @Override
            public PVector<V> apply(PVector<V> old) {
                List<V> dropped = drop.apply(old.plus(value));
                return TreePVector.from(dropped);
            }
        });

        firehose.notify(destination.clone(src), newv);
    }
}
