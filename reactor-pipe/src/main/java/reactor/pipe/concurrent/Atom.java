package reactor.pipe.concurrent;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import reactor.core.tuple.Tuple2;

/**
 * Generic Atom
 */
public class Atom<T> {

    private final AtomicReference<T> ref;
    private final Consumer<T>        newValueConsumer;

    public Atom(T ref) {
        this(ref, null);
    }

    // TODO: make atom an intereace, extract consumer impl elsewhere
    public Atom(T ref,
                Consumer<T> newValueConsumer) {
        this.ref = new AtomicReference<T>(ref);
        this.newValueConsumer = newValueConsumer;
    }

    public T deref() {
        return ref.get();
    }

    public T update(UnaryOperator<T> swapOp) {
        for (; ; ) {
            T old = ref.get();
            T newv = swapOp.apply(old);
            if (ref.compareAndSet(old, newv)) {
                if (newValueConsumer != null && !newv.equals(old)) {
                    newValueConsumer.accept(newv);
                }
                return newv;
            }
        }
    }

    public T updateAndReturnOld(UnaryOperator<T> swapOp) {
        for (; ; ) {
            T old = ref.get();
            T newv = swapOp.apply(old);
            if (ref.compareAndSet(old, newv)) {
                return old;
            }
        }
    }

    public <O> O updateAndReturnOther(Function<T, Tuple2<T, O>> swapOp) {
        for (; ; ) {
            T old = ref.get();
            Tuple2<T, O> newvtuple = swapOp.apply(old);
            if (ref.compareAndSet(old, newvtuple.getT1())) {
                return newvtuple.getT2();
            }
            LockSupport.parkNanos(1L);
        }
    }

    public <O> O updateAndReturnOther(Predicate<T> pred,
                                      Function<T, Tuple2<T, O>> swapOp) {
        for (; ; ) {
            T old = ref.get();
            if (pred.test(old)) {
                Tuple2<T, O> newvtuple = swapOp.apply(old);
                if (ref.compareAndSet(old, newvtuple.getT1())) {
                    return newvtuple.getT2();
                }
                LockSupport.parkNanos(1L); //TODO: Maybe park everywhere?
            }
        }
    }

    public T reset(T newv) {
        for (; ; ) {
            T old = ref.get();
            if (ref.compareAndSet(old, newv)) {
                return newv;
            }
        }
    }


}
