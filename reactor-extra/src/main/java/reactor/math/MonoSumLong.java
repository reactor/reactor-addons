/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.math;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.Fuseable;
import reactor.core.publisher.MonoSource;

/**
 * Computes the sum of source numbers and returns the result as a long.
 *
 * @param <T> the input value type
 */
final class MonoSumLong<T> extends MonoSource<T, Long> implements Fuseable {

	final Function<? super T, ? extends Number> mapping;

	MonoSumLong(Publisher<? extends T> source, Function<? super T, ? extends Number> mapping) {
		super(source);
		this.mapping = mapping;
	}

	@Override
	public void subscribe(Subscriber<? super Long> s) {
		source.subscribe(new SumLongSubscriber<T>(s, mapping));
	}

	static final class SumLongSubscriber<T> extends MathSubscriber<T, Long> {

		final Function<? super T, ? extends Number> mapping;

		long sum;

		boolean hasValue;

		SumLongSubscriber(Subscriber<? super Long> actual, Function<? super T, ? extends Number> mapping) {
			super(actual);
			this.mapping = mapping;
		}

		@Override
		protected void updateResult(T newValue) {
			long longValue = mapping.apply(newValue).longValue();
			sum = hasValue ? sum + longValue : longValue;
			hasValue = true;
		}

		@Override
		protected Long result() {
			return hasValue ? sum : null;
		}

		@Override
		protected void reset() {
			sum = 0L;
			hasValue = false;
		}
	}
}
