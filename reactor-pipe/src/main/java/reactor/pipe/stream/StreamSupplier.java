package reactor.pipe.stream;

import java.util.function.BiConsumer;

import reactor.bus.Bus;
import reactor.pipe.key.Key;

@FunctionalInterface
public interface StreamSupplier<K extends Key, V> {
    BiConsumer<K, V> get(K src,
                         Key dst,
                         Bus<Key, Object> firehose);
}
