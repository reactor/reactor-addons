package reactor.pipe.operation;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.bus.Bus;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

public class PartitionOperation<SRC extends Key, DST extends Key, V> implements BiConsumer<SRC, V> {

    private final Atom<PVector<V>>   buffer;
    private final Bus<Key, Object>   firehose;
    private final Predicate<List<V>> emit;
    private final DST                destination;

    public PartitionOperation(Bus<Key, Object> firehose,
                              Atom<PVector<V>> buffer,
                              Predicate<List<V>> emit,
                              DST destination) {
        this.buffer = buffer;
        this.firehose = firehose;
        this.emit = emit;
        this.destination = destination;
    }

    @Override
    @SuppressWarnings(value = {"unchecked"})
    public void accept(final SRC key, final V value) {
        PVector<V> newv = buffer.update(new UnaryOperator<PVector<V>>() {
            @Override
            public PVector<V> apply(PVector<V> old) {
                return old.plus(value);
            }
        });

        if (emit.test(newv)) {
            PVector<V> downstreamValue = buffer.updateAndReturnOld(new UnaryOperator<PVector<V>>() {
                @Override
                public PVector<V> apply(PVector<V> vs) {
                    return TreePVector.empty();
                }
            });
            firehose.notify(destination.clone(key), downstreamValue);
        }
    }
}
