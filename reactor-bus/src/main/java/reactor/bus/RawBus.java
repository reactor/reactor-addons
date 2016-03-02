/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.bus;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import reactor.bus.filter.PassThroughFilter;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.routing.ConsumerFilteringRouter;
import reactor.bus.routing.Router;
import reactor.bus.selector.Selector;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.Subscribers;
import reactor.core.subscriber.SubscriptionWithContext;
import reactor.core.util.*;
import reactor.rx.Fluxion;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} {@link Consumer}s that can
 * subsequently be notified of events. A consumer is typically registered with a {@link Selector} which, by matching on
 * the notification key, governs which events the consumer will receive. </p> When a {@literal Reactor} is notified of
 * an {@link Event}, a task is dispatched using the reactor's {@link Processor} which causes it to be executed
 * on a
 * thread based on the implementation of the {@link Processor} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 * @author Alex Petrov
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class RawBus<K, V> implements Bus<K, V>, MultiProducer {

	protected static final Router DEFAULT_EVENT_ROUTER = new ConsumerFilteringRouter(
		new PassThroughFilter()
	);

	private final Processor<Runnable, Runnable>           processor;
	private final Registry<K, BiConsumer<K, ? extends V>> consumerRegistry;
	private final Router                                  router;
	private final Consumer<Throwable>                     processorErrorHandler;
	private final Consumer<Throwable>                     uncaughtErrorHandler;
	private final int                                     concurrency;

	private volatile UUID id;

	/**
	 * Create a new {@literal Reactor} that uses the given {@code processor} and {@code eventRouter}.
	 *
	 * @param consumerRegistry      The {@link Registry} to be used to match {@link Selector} and dispatch to {@link
	 *                              Consumer}
	 * @param concurrency           The allowed number of concurrent routing. This is highly dependent on the
	 *                              processor used. Only "Work" processors like {@link reactor.core.publisher
	 *                              .WorkQueueProcessor} will be meaningful as they distribute their messages,
	 *                              default RS behavior is to broadcast resulting in a matching number of duplicate
	 *                              routing.
	 * @param router                The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code
	 *                              null} in which case the default event router that broadcasts events to all of the
	 *                              registered consumers that {@link
	 *                              Selector#matches(Object) match} the notification key and does not perform any type
	 *                              conversion will be used.
	 * @param processorErrorHandler The {@link Consumer} to be used on {@link Processor} exceptions. May be {@code null}
	 *                              in which case exceptions will be delegated to {@code uncaughtErrorHandler}.
	 * @param uncaughtErrorHandler  Default {@link Consumer} to be used on all uncaught exceptions. May be {@code null}
	 *                              in which case exceptions will be logged.
	 */
	@SuppressWarnings("unchecked")
	public RawBus(@Nonnull Registry<K, BiConsumer<K, ? extends V>> consumerRegistry,
				  Processor<Runnable, Runnable> processor,
				  int concurrency,
				  @Nullable Router router,
				  @Nullable Consumer<Throwable> processorErrorHandler,
				  @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		Assert.notNull(consumerRegistry, "Consumer Registry cannot be null.");
		this.consumerRegistry = consumerRegistry;
		this.concurrency = concurrency;
		this.router = (null == router ? DEFAULT_EVENT_ROUTER : router);
		if (null == processorErrorHandler) {
			this.processorErrorHandler = new Consumer<Throwable>() {
				@Override
				public void accept(Throwable t) {
					if (uncaughtErrorHandler == null) {
						final Logger log = Logger.getLogger(RawBus.class);
						log.error(t.getMessage(), t);
					} else {
						uncaughtErrorHandler.accept(t);
					}
				}
			};
		} else {
			this.processorErrorHandler = processorErrorHandler;
		}

		this.uncaughtErrorHandler = uncaughtErrorHandler;

		this.processor = processor;
		if (this.processor != null) {
			for (int i = 0; i < concurrency; i++) {
				this.processor.subscribe(
					Subscribers.unbounded(new BiConsumer<Runnable, SubscriptionWithContext<Void>>() {
											  @Override
											  public void accept(Runnable runnable,
																 SubscriptionWithContext<Void> voidSubscriptionWithContext) {
												  runnable.run();
											  }
										  },
										  uncaughtErrorHandler));
			}
			this.processor.onSubscribe(EmptySubscription.INSTANCE);
		}

	}

	/**
	 * Get the {@link Processor} currently in use.
	 *
	 * @return The {@link Processor}.
	 */
	public Processor<Runnable, Runnable> getProcessor() {
		return this.processor;
	}

	/**
	 * Get the unique, time-used {@link UUID} of this {@literal Reactor}.
	 *
	 * @return The {@link UUID} of this {@literal Reactor}.
	 */
	public synchronized UUID getId() {
		if (null == id) {
			id = UUIDUtils.create();
		}
		return id;
	}

	/**
	 * Get the {@link Registry} is use to maintain the {@link Consumer}s currently listening for events on this
	 * {@literal
	 * Reactor}.
	 *
	 * @return The {@link Registry} in use.
	 */
	public Registry<K, BiConsumer<K, ? extends V>> getConsumerRegistry() {
		return consumerRegistry;
	}

	/**
	 * Get the {@link Router} used to route events to {@link Consumer Consumers}.
	 *
	 * @return The {@link Router}.
	 */
	public Router getRouter() {
		return router;
	}

	/**
	 * Get the {@link Consumer<Throwable>} processor error handler
	 *
	 * @return The {@link Consumer<Throwable>} processor error handler in use
	 */
	public Consumer<Throwable> getProcessorErrorHandler() {
		return processorErrorHandler;
	}

	/**
	 * Get the {@link Consumer<Throwable>} uncaught error handler
	 *
	 * @return The {@link Consumer<Throwable>} uncaught error handler in use
	 */
	public Consumer<Throwable> getUncaughtErrorHandler() {
		return uncaughtErrorHandler;
	}

	@Override
	public boolean respondsToKey(K key) {
		List<Registration<K, ? extends BiConsumer<K, ? extends V>>> registrations = consumerRegistry.select(key);

		if (registrations.isEmpty())
			return false;

		for (Registration<?, ?> reg : registrations) {
			if (!reg.isCancelled()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public <T extends V> Registration<K, BiConsumer<K, ? extends V>> on(final Selector selector,
																		final BiConsumer<K, T> consumer) {
		Assert.notNull(selector, "Selector cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");

		return consumerRegistry.register(selector, consumer);
	}

	@Override
	public <V1 extends V> Registration<K, BiConsumer<K, ? extends V>> on(final Selector selector,
																		 final Consumer<V1> consumer) {
		return on(selector, new BusConsumer(consumer));
	}

	@Override
	public <V1 extends V> Registration<K, BiConsumer<K, ? extends V>> onKey(final K key,
																			final BiConsumer<K, V1> consumer) {
		Assert.notNull(key, "Key cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");

		return consumerRegistry.register(key, consumer);
	}

	@Override
	public <T extends V> Registration<K, BiConsumer<K, ? extends V>> onKey(final K key,
																		   final Consumer<T> consumer) {
		return onKey(key, new BusConsumer(consumer));
	}

	/**
	 * Concurrency level of the Bus
	 *
	 * @return concurrency level
	 */
	public int getConcurrency() {
		return concurrency;
	}

	/**
	 * Attach a Publisher to the {@link Bus} with the specified {@link Selector}.
	 *
	 * @param broadcastSelector the {@link Selector}/{@literal Object} tuple to listen to
	 * @return a new {@link Publisher}
	 * @since 2.0
	 */
	public Fluxion<? extends V> on(Selector broadcastSelector) {
		return new BusFluxion<>(this, broadcastSelector);
	}

	protected void accept(final K key, final V value) {
		if (processor == null) {
			route(key, value);
		} else {
			processor.onNext(new Runnable() {
				@Override
				public void run() {
					try {
						route(key, value);
					} catch (Throwable outer) {
						errorHandlerOrThrow(new RuntimeException("Exception in key: " + key.toString(), outer));
					}
				}
			});
		}
	}

	@Override
	public RawBus notify(final K key, final V value) {
		Assert.notNull(key, "Key cannot be null.");
		Assert.notNull(value, "Event cannot be null.");

		accept(key, value);

		return this;
	}

	@Override
	public RawBus notify(K key, Supplier<? extends V> supplier) {
		return notify(key, supplier.get());
	}

	@Override
	public Iterator<?> downstreams() {
		return consumerRegistry.iterator();
	}

	@Override
	public long downstreamCount() {
		return consumerRegistry.size();
	}

	protected void errorHandlerOrThrow(Throwable t) {
		if (processorErrorHandler != null) {
			Exceptions.throwIfFatal(t);
			processorErrorHandler.accept(t);
		} else {
			Exceptions.onErrorDropped(t);
		}
	}

	protected void route(K key, V value) {
		router.route(key, value, consumerRegistry.select(key), null, processorErrorHandler);
	}

	private static class BusConsumer<K, T> implements BiConsumer<K, T>, Introspectable, Producer {

		private final Consumer<T> consumer;

		public BusConsumer(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		public Object downstream() {
			return consumer;
		}

		@Override
		public void accept(K k, T v) {
			consumer.accept(v);
		}

		@Override
		public int getMode() {
			return TRACE_ONLY;
		}

		@Override
		public String getName() {
			return BusConsumer.class.getSimpleName();
		}
	}
}
