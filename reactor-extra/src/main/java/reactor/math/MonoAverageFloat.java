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
 * Computes the average of source numbers and returns the result as a float.
 *
 * @param <T> the input value type
 */
final class MonoAverageFloat<T> extends MonoFromFluxOperator<T, Float> implements Fuseable {

	final Function<? super T, ? extends Number> mapping;

	MonoAverageFloat(Publisher<? extends T> source, Function<? super T, ? extends Number> mapping) {
		super(Flux.from(source));
		this.mapping = mapping;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Float> s) {
		source.subscribe(new AverageFloatSubscriber<T>(s, mapping));
	}

	static final class AverageFloatSubscriber<T> extends MathSubscriber<T, Float> {

		final Function<? super T, ? extends Number> mapping;

		int count;

		float sum;

		AverageFloatSubscriber(CoreSubscriber<? super Float> actual, Function<? super T, ? extends Number> mapping) {
			super(actual);
			this.mapping = mapping;
		}

		@Override
		protected void updateResult(T newValue) {
			count++;
			float floatValue = mapping.apply(newValue).floatValue();
			sum += floatValue;
		}

		@Override
		protected @Nullable Float result() {
			return count == 0 ? null : sum / count;
		}

		@Override
		protected void reset() {
			count = 0;
			sum = 0.0F;
		}
	}
}
