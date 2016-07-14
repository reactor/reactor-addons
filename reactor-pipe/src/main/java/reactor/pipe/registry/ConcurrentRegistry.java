package reactor.pipe.registry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selector;
import reactor.util.function.Tuple;
import reactor.util.function.Tuple2;
import reactor.pipe.concurrent.Atom;

public class ConcurrentRegistry<K, V> implements Registry<K, V> {

    private final Atom<PMap<K, PVector<Registration<K, V>>>> exactKeyMatches;
    // This one can't be map, since key miss matcher is a possibly non-capturing lambda,
    // So we have no other means to work around the uniqueness
    private final List<Registration<K, V>>                   keyMissMatchers;

    public ConcurrentRegistry() {
        this.exactKeyMatches = new Atom<>(HashTreePMap.empty());
        this.keyMissMatchers = new ArrayList<>();
    }

    @Override
    public Registration<K, V> register(Selector<K> matcher, V value) {
        Registration<K, V> registration = new DelayedRegistration<>(matcher, value);
        this.keyMissMatchers.add(registration);
        return registration;
    }

    @Override
    public Registration<K, V> register(final K key, V obj) {
        final PVector<Registration<K, V>> lookedUpArr = exactKeyMatches.deref().get(key);
        final Registration<K, V> reg = new KeyRegistration<>(obj);
        if (lookedUpArr == null) {
            exactKeyMatches.update(new UnaryOperator<PMap<K, PVector<Registration<K, V>>>>() {
                @Override
                public PMap<K, PVector<Registration<K, V>>> apply(PMap<K, PVector<Registration<K, V>>> old) {
                    return old.plus(key, TreePVector.singleton(reg));
                }
            });
        } else {
            exactKeyMatches.update(new UnaryOperator<PMap<K, PVector<Registration<K, V>>>>() {
                @Override
                public PMap<K, PVector<Registration<K, V>>> apply(PMap<K, PVector<Registration<K, V>>> old) {
                    return old.plus(key, old.get(key).plus(reg));
                }
            });
        }
        return reg;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Registration<K, ? extends V>> select(K key) {
        PVector<Registration<K, V>> registrations = exactKeyMatches.deref().get(key);
        if (null == registrations || registrations.isEmpty()) {
            List<Registration<K, ? extends V>> result = new ArrayList<>();
            for (Registration<K, ? extends V> reg : keyMissMatchers) {
                if (reg.getSelector().matches(key)) {
                    result.add(reg);
                }
            }
            return result;
        } else {
            Registration<K, ? extends V>[] result = new Registration[registrations.size()];
            registrations.toArray(result);
            return Arrays.asList(result);
        }
    }

    @Override
    public boolean unregister(K key) {
        return exactKeyMatches.updateAndReturnOther(
          new Function<PMap<K, PVector<Registration<K, V>>>, Tuple2<PMap<K, PVector<Registration<K, V>>>, Boolean>>() {
              @Override
              public Tuple2<PMap<K, PVector<Registration<K, V>>>, Boolean> apply(
                PMap<K, PVector<Registration<K, V>>> m) {
                  PMap<K, PVector<Registration<K, V>>> newMap = m.minus(key);
                  return Tuple.of(newMap, !m.containsKey(key));
              }
          });
    }


    @Override
    public Iterable<? extends V> selectValues(K key) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public long size() {
        return exactKeyMatches.deref().size();
    }

    @Override
    public Iterator<Registration<K, ? extends V>> iterator() {
        return null;
    }


}
