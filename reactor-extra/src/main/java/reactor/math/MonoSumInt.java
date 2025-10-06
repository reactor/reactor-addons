/*
 * Copyright (c) 2017-2025 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.math;

import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;

/**
 * Computes the sum of source numbers and returns the result as an integer.
 *
 * @param <T> the input value type
 */
final class MonoSumInt<T> extends MonoFromFluxOperator<T, Integer> implements Fuseable {

	final Function<? super T, ? extends Number> mapping;

	MonoSumInt(Publisher<? extends T> source, Function<? super T, ? extends Number> mapping) {
		super(Flux.from(source));
		this.mapping = mapping;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Integer> s) {
		source.subscribe(new SumIntSubscriber<T>(s, mapping));
	}

	static final class SumIntSubscriber<T> extends MathSubscriber<T, Integer> {

		final Function<? super T, ? extends Number> mapping;

		int sum;

		boolean hasValue;

		SumIntSubscriber(CoreSubscriber<? super Integer> actual, Function<? super T, ? extends Number> mapping) {
			super(actual);
			this.mapping = mapping;
		}

		@Override
		protected void updateResult(T newValue) {
			int intValue = mapping.apply(newValue).intValue();
			if (hasValue) {
				boolean sumPositive = sum >= 0;
				sum = sum + intValue;
				//overflow
				if (sumPositive && intValue >= 0 && sum < 0) {
					sum = Integer.MAX_VALUE;
				}
				//underflow
				else if (!sumPositive && intValue < 0 && sum > 0) {
					sum = Integer.MIN_VALUE;
				}
			}
			else {
				sum = intValue;
				hasValue = true;
			}
		}

		@Override
		protected @Nullable Integer result() {
			return hasValue ? sum : null;
		}

		@Override
		protected void reset() {
			sum = 0;
			hasValue = false;
		}
	}
}
