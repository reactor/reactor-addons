package reactor.pipe;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.reactivestreams.Processor;
import reactor.bus.AbstractBus;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.routing.Router;
import reactor.core.publisher.Flux;
import reactor.pipe.registry.DelayedRegistration;
import reactor.pipe.stream.FirehoseSubscription;

public class RawBus<K, V> extends AbstractBus<K, V> {

    private final Processor<Runnable, Runnable> processor;
    private final ThreadLocal<Boolean>          inDispatcherContext;
    private final FirehoseSubscription          firehoseSubscription;

    public RawBus(@Nonnull final Registry<K, BiConsumer<K, ? extends V>> consumerRegistry,
                  @Nullable Processor<Runnable, Runnable> processor,
                  int concurrency,
                  @Nullable final Router router,
                  @Nullable Consumer<Throwable> processorErrorHandler,
                  @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
        super(consumerRegistry,
              concurrency,
              router,
              processorErrorHandler,
              uncaughtErrorHandler);
        this.processor = processor;
        this.inDispatcherContext = new ThreadLocal<Boolean>(){
          @Override
          protected Boolean initialValue() {
            return Boolean.FALSE;
          }
        };
        this.firehoseSubscription = new FirehoseSubscription();

        if (processor != null) {
            Flux<Runnable> p = Flux.from(processor);
            for (int i = 0; i < concurrency; i++) {
                p.subscribe(Runnable::run, uncaughtErrorHandler);
            }
            processor.onSubscribe(firehoseSubscription);
        }
    }

    @Override
    protected void accept(final K key, final V value) {
        if (inDispatcherContext.get()) {
            // Since we're already in the context, we should route syncronously
            try {
                route(key, value);
            } catch (Throwable outer) {
                errorHandlerOrThrow(outer);
            }
        } else {
            // Backpressure
            while (!firehoseSubscription.maybeClaimSlot()) {
              try {
                //LockSupport.parkNanos(10000);
                Thread.sleep(10); // TODO: Migrate to parknanos
              } catch (InterruptedException e) {
                errorHandlerOrThrow(e);
              }
            }

            processor.onNext(new Runnable() {
                @Override
                public void run() {
                    try {
                        inDispatcherContext.set(Boolean.TRUE);
                        route(key, value);
                    } catch (Throwable outer) {
                        errorHandlerOrThrow(new RuntimeException("Exception in key: " + key.toString(), outer));
                    } finally {
                        inDispatcherContext.set(Boolean.FALSE);
                    }
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    protected void route(K key, V value) {
        List<Registration<K, ? extends BiConsumer<K, ? extends V>>> registrations = getConsumerRegistry().select(key);

        if (registrations.isEmpty()) {
            return;
        }

        if (registrations.get(0) instanceof DelayedRegistration) {
            getRouter().route(key, value, registrations, null, getProcessorErrorHandler());
            getRouter().route(key, value, getConsumerRegistry().select(key), null, getProcessorErrorHandler());
        } else {
            getRouter().route(key, value, registrations, null, getProcessorErrorHandler());
        }
    }

}
