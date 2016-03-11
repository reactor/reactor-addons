/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus.registry;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import reactor.bus.selector.ObjectSelector;
import reactor.bus.selector.Selector;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Implementation of {@link Registry} that uses a partitioned cache that partitions on thread
 * id.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Oleksandr Petrov
 */
public class CachingRegistry<K, V> implements Registry<K, V> {

	private final Consumer<K>                                              onNotFound;
	private final MultiReaderFastList<Registration<K, ? extends V>>        registrations;
	private final ConcurrentHashMap<K, List<Registration<K, ? extends V>>> exactKeyMatches;

	CachingRegistry(Consumer<K> onNotFound) {
		this.onNotFound = onNotFound;
		this.registrations = MultiReaderFastList.newList();
		this.exactKeyMatches = new ConcurrentHashMap<K, List<Registration<K, ? extends V>>>();
	}

	@Override
	public Registration<K, V> register(Selector<K> sel, V obj) {
		RemoveRegistration removeFn = new RemoveRegistration();
		final Registration<K, V> reg = new CachableRegistration<>(sel, obj, removeFn);
		removeFn.reg = reg;

		registrations.withWriteLockAndDelegate(new Procedure<MutableList<Registration<K, ? extends V>>>() {
			@Override
			public void value(MutableList<Registration<K, ? extends V>> regs) {
				regs.add(reg);
			}
		});

		return reg;
	}

	@Override
	public Registration<K, V> register(K sel, V obj) {
		return register(new ObjectSelector<K, K>(sel), obj);
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean unregister(final K key) {
		final AtomicBoolean modified = new AtomicBoolean(false);
		registrations.withWriteLockAndDelegate(new Procedure<MutableList<Registration<K, ? extends V>>>() {
			@Override
			public void value(final MutableList<Registration<K, ? extends V>> regs) {
				Iterator<Registration<K, ? extends V>> registrationIterator = regs.iterator();
				Registration<K, ? extends V> reg;
				while (registrationIterator.hasNext()) {
					reg = registrationIterator.next();
					if (reg.getSelector().matches(key)) {
						registrationIterator.remove();
						modified.compareAndSet(false, true);
					}
				}
				exactKeyMatches.remove(key);
			}
		});
		return modified.get();
	}

	@Override
	public Iterable<? extends V> selectValues(final K key) {
		return SimpleCachingRegistry.selectValues(this, key);
	}

	@Override
	public List<Registration<K, ? extends V>> select(K key) {
		List<Registration<K, ? extends V>> selectedRegs = exactKeyMatches.get(key);

		if (selectedRegs == null || selectedRegs.isEmpty()) {
			List<Registration<K, ? extends V>> delayedRegistrations =
				registrations.select(reg -> reg.getSelector().matches(key));

			selectedRegs = exactKeyMatches.compute(key,
												   (k_, regs) -> {
													   if (regs == null) {
														   return delayedRegistrations;
													   } else {
														   regs.addAll(delayedRegistrations);
														   return regs;
													   }
												   });

			if (selectedRegs.isEmpty()) {
				onNotFound.accept(key);
			}
			return selectedRegs;
		} else {
			return selectedRegs;
		}
	}

	@Override
	public void clear() {
		registrations.clear();
		exactKeyMatches.clear();
	}

	@Override
	public long size() {
		return registrations.size();
	}

	@Override
	public Iterator<Registration<K, ? extends V>> iterator() {
		return FastList.newList(registrations).iterator();
	}

	private final class RemoveRegistration implements Runnable {
		Registration<K, ? extends V> reg;

		@Override
		public void run() {
			registrations.withWriteLockAndDelegate(new Procedure<MutableList<Registration<K, ? extends V>>>() {
				@Override
				public void value(MutableList<Registration<K, ? extends V>> regs) {
					regs.remove(reg);
					// TODO: cleanup direct lookups
				}
			});
		}
	}

	private final class NewThreadLocalRegsFn
		implements Function<Long, UnifiedMap<Object, List<Registration<K, ? extends V>>>> {
		@Override
		public UnifiedMap<Object, List<Registration<K, ? extends V>>> apply(Long aLong) {
			return UnifiedMap.newMap();
		}
	}

}
