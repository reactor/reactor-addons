/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.math.BigInteger;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;

/**
 * Computes the average of source numbers and returns the result as a {@link BigInteger}.
 *
 * @param <T> the input value type
 */
public class MonoAverageBigInteger<T> extends MonoFromFluxOperator<T, BigInteger>
		implements Fuseable {

	private final Function<? super T, ? extends Number> mapping;

	MonoAverageBigInteger(Publisher<T> source,
			Function<? super T, ? extends Number> mapping) {
		super(Flux.from(source));
		this.mapping = mapping;
	}

	@Override
	public void subscribe(CoreSubscriber<? super BigInteger> actual) {
		source.subscribe(new AverageBigIntegerSubscriber<T>(actual, mapping));
	}

	private static final class AverageBigIntegerSubscriber<T>
			extends MathSubscriber<T, BigInteger> {

		private final Function<? super T, ? extends Number> mapping;

		private int count;

		private BigInteger sum = BigInteger.ZERO;

		AverageBigIntegerSubscriber(CoreSubscriber<? super BigInteger> actual,
				Function<? super T, ? extends Number> mapping) {
			super(actual);
			this.mapping = mapping;
		}

		@Override
		protected void reset() {
			count = 0;
			sum = BigInteger.ZERO;
		}

		@Override
		protected BigInteger result() {
			return (count == 0 ? null : sum.divide(BigInteger.valueOf(count)));
		}

		@Override
		protected void updateResult(T newValue) {
			Number number = mapping.apply(newValue);
			BigInteger bigIntegerValue = BigInteger.valueOf(number.longValue());
			sum = sum.add(bigIntegerValue);
			count++;
		}
	}
}
