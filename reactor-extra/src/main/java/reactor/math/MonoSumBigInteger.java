/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;

/**
 * Computes the sun of source numbers and returns the result as {@link BigInteger}
 *
 * @param <T> the input value type
 */
final class MonoSumBigInteger<T> extends MonoFromFluxOperator<T, BigInteger>
		implements Fuseable {

	private final Function<? super T, ? extends Number> mapping;

	MonoSumBigInteger(Publisher<? extends T> source,
			Function<? super T, ? extends Number> mapping) {
		super(Flux.from(source));
		this.mapping = mapping;
	}

	@Override
	public void subscribe(CoreSubscriber<? super BigInteger> actual) {
		source.subscribe(new SumBigIntegerSubscriber<T>(actual, mapping));
	}

	static final private class SumBigIntegerSubscriber<T>
			extends MathSubscriber<T, BigInteger> {

		private final Function<? super T, ? extends Number> mapping;

		BigDecimal sum;

		boolean hasValue;

		SumBigIntegerSubscriber(CoreSubscriber<? super BigInteger> actual,
				Function<? super T, ? extends Number> mapping) {
			super(actual);
			this.mapping = mapping;
		}

		@Override
		protected void reset() {
			sum = BigDecimal.ZERO;
			hasValue = false;
		}

		@Override
		protected BigInteger result() {
			return (hasValue ? sum.toBigInteger() : null);
		}

		@Override
		protected void updateResult(T newValue) {
			Number number = mapping.apply(newValue);
			BigDecimal bigDecimalValue;
			if (number instanceof BigDecimal) {
				bigDecimalValue = (BigDecimal) number;
			}
			else if (number instanceof BigInteger) {
				bigDecimalValue = new BigDecimal((BigInteger) number);
			}
			else {
				bigDecimalValue = new BigDecimal(number.toString());
			}
			sum = hasValue ? sum.add(bigDecimalValue) : bigDecimalValue;
			hasValue = true;
		}
	}
}
