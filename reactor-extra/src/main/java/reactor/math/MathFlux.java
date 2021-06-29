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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;

/**
 * Mathematical utilities that compute sum, average, minimum or maximum values
 * from numerical sources or sources that can be mapped to numerical values using
 * custom mappings. Minimum and maximum values can be computed for any source
 * containing {@link Comparable} values or using custom {@link Comparator}.
 *
 */
public final class MathFlux {
	
	private MathFlux() {
	}

	/**
	 * Computes the integer sum of items in the source. Note that in case of an overflow,
	 * this method won't return a negative sum but rather cap at {@link Integer#MAX_VALUE}
	 * (or {@link Integer#MIN_VALUE} for an underflow).
	 *
	 * @param source the numerical source
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static Mono<Integer> sumInt(Publisher<? extends Number> source) {
		return sumInt(source, i -> i);
	}

	/**
	 * Computes the integer sum of items in the source, which are mapped to numerical values
	 * using the provided mapping. Note that in case of an overflow,
	 * this method won't return a negative sum but rather cap at {@link Integer#MAX_VALUE}
	 * (or {@link Integer#MIN_VALUE} for an underflow).
	 *
	 * @param source the source items
	 * @param mapping a function to map source items to numerical values
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static final <T> Mono<Integer> sumInt(Publisher<T> source, Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoSumInt<T>(source, mapping));
	}

	/**
	 * Computes the long sum of items in the source. Note that in case of an overflow,
	 * this method won't return a negative sum but rather cap at {@link Long#MAX_VALUE}
	 * (or {@link Long#MIN_VALUE} for an underflow).
	 *
	 * @param source the numerical source
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static Mono<Long> sumLong(Publisher<? extends Number> source) {
		return sumLong(source, i -> i);
	}

	/**
	 * Computes the long sum of items in the source, which are mapped to numerical values
	 * using the provided mapping. Note that in case of an overflow,
	 * this method won't return a negative sum but rather cap at {@link Long#MAX_VALUE}
	 * (or {@link Long#MIN_VALUE} for an underflow).
	 *
	 * @param source the source items
	 * @param mapping a function to map source items to numerical values
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static final <T> Mono<Long> sumLong(Publisher<T> source, Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoSumLong<T>(source, mapping));
	}

	/**
	 * Computes the float sum of items in the source.
	 *
	 * @param source the numerical source
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static Mono<Float> sumFloat(Publisher<? extends Number> source) {
		return sumFloat(source, i -> i);
	}

	/**
	 * Computes the float sum of items in the source, which are mapped to numerical values
	 * using the provided mapping.
	 *
	 * @param source the source items
	 * @param mapping a function to map source items to numerical values
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static final <T> Mono<Float> sumFloat(Publisher<T> source, Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoSumFloat<T>(source, mapping));
	}

	/**
	 * Computes the double sum of items in the source.
	 *
	 * @param source the numerical source
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static Mono<Double> sumDouble(Publisher<? extends Number> source) {
		return sumDouble(source, i -> i);
	}

	/**
	 * Computes the double sum of items in the source, which are mapped to numerical values
	 * using the provided mapping.
	 *
	 * @param source the source items
	 * @param mapping a function to map source items to numerical values
	 *
	 * @return {@link Mono} of the sum of items in source
	 */
	public static final <T> Mono<Double> sumDouble(Publisher<T> source, Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoSumDouble<T>(source, mapping));
	}

	/**
	 * Computes the {@link BigInteger} sum of items in the source.
	 *
	 * @param source the numerical source
	 * @return {@link Mono} of the sum of items in source
	 */
	public static Mono<BigInteger> sumBigInteger(Publisher<? extends Number> source) {
		return sumBigInteger(source, i -> i);
	}

	/**
	 * Computes the {@link BigInteger} sum of items in the source, which are mapped to
	 * numerical values using provided mapping.
	 *
	 * @param source  the source items
	 * @param mapping a function to map source items to numerical values
	 * @return {@link Mono} of sum of items in source
	 */
	public static final <T> Mono<BigInteger> sumBigInteger(Publisher<T> source,
			Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoSumBigInteger<T>(source, mapping));
	}

	/**
	 * Computes the {@link BigDecimal} sum of items in the source.
	 *
	 * @param source the numerical source
	 * @return {@link Mono} of the sum of items in source
	 */
	public static Mono<BigDecimal> sumBigDecimal(Publisher<? extends Number> source) {
		return sumBigDecimal(source, i -> i);
	}

	/**
	 * Computes the {@link BigDecimal} sum of items in the source, which are
	 * mapped to numerical values using provided mapping.
	 *
	 * @param source  the source items
	 * @param mapping a function to map source items to numerical values
	 * @return {@link Mono} of sum of items in source
	 */
	public static final <T> Mono<BigDecimal> sumBigDecimal(Publisher<T> source,
			Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoSumBigDecimal<T>(source, mapping));
	}

	/**
	 * Computes the float average of items in the source.
	 *
	 * @param source the numerical source
	 *
	 * @return {@link Mono} of the average of items in source
	 */
	public static Mono<Float> averageFloat(Publisher<? extends Number> source) {
		return averageFloat(source, i -> i);
	}

	/**
	 * Computes the float average of items in the source, which are mapped to numerical values
	 * using the provided mapping.
	 *
	 * @param source the source items
	 * @param mapping a function to map source items to numerical values
	 *
	 * @return {@link Mono} of the average of items in source
	 */
	public static final <T> Mono<Float> averageFloat(Publisher<T> source, Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoAverageFloat<T>(source, mapping));
	}

	/**
	 * Computes the double average of items in the source.
	 *
	 * @param source the numerical source
	 *
	 * @return {@link Mono} of the average of items in source
	 */
	public static Mono<Double> averageDouble(Publisher<? extends Number> source) {
		return averageDouble(source, i -> i);
	}

	/**
	 * Computes the double average of items in the source, which are mapped to numerical values
	 * using the provided mapping.
	 *
	 * @param source the source items
	 * @param mapping a function to map source items to numerical values
	 *
	 * @return {@link Mono} of the average of items in source
	 */
	public static final <T> Mono<Double> averageDouble(Publisher<T> source, Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoAverageDouble<T>(source, mapping));
	}

	/**
	 * Computes the {@link BigInteger} average of items in the source.
	 *
	 * @param source the numerical source
	 * @return {@link Mono} of the average of items in source
	 */
	public static Mono<BigInteger> averageBigInteger(Publisher<? extends Number> source) {
		return averageBigInteger(source, i -> i);
	}

	/**
	 * Computes the {@link BigInteger} average of items in the source, which are mapped to
	 * numerical values using the provided mapping.
	 *
	 * @param source  the source items
	 * @param mapping a function to map source items to numerical values
	 * @return {@link Mono} of the average of items in source
	 */
	public static final <T> Mono<BigInteger> averageBigInteger(Publisher<T> source,
			Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoAverageBigInteger<T>(source, mapping));
	}


	/**
	 * Computes the {@link BigDecimal} average of items in the source.
	 *
	 * @param source the numerical source
	 * @return {@link Mono} of the average of items in source
	 */
	public static Mono<BigDecimal> averageBigDecimal(Publisher<? extends Number> source) {
		return averageBigDecimal(source, i -> i);
	}

	/**
	 * Computes the {@link BigDecimal} average of items in the source, which are mapped to
	 * numerical values using the provided mapping.
	 *
	 * @param source  the source items
	 * @param mapping a function to map source items to numerical values
	 * @return {@link Mono} of the average of items in source
	 */
	public static final <T> Mono<BigDecimal> averageBigDecimal(Publisher<T> source,
			Function<? super T, ? extends Number> mapping) {
		return MathMono.onAssembly(new MonoAverageBigDecimal<T>(source, mapping));
	}

	/**
	 * Computes the maximum value of items in the source.
	 *
	 * @param source the source containing comparable items
	 *
	 * @return {@link Mono} of the maximum value in source
	 */
	public static final <T extends Comparable<? super T>> Mono<T> max(Publisher<T> source) {
		Comparator<? super T> comparator = comparableComparator();
		return max(source, comparator);
	}

	/**
	 * Computes the maximum value of items in the source.
	 *
	 * @param source the source containing items to compare
	 * @param comparator the comparator used to compare the items in source
	 *
	 * @return {@link Mono} of the maximum value in source
	 */
	public static <T> Mono<T> max(Publisher<T> source, Comparator<? super T> comparator) {
		return MathMono.onAssembly(new MonoMinMax<T>(source, comparator, 1));
	}

	/**
	 * Computes the minimum value of items in the source.
	 *
	 * @param source the source containing comparable items
	 *
	 * @return {@link Mono} of the minimum value in source
	 */
	public static final <T extends Comparable<? super T>> Mono<T> min(Publisher<T> source) {
		Comparator<? super T> comparator = comparableComparator();
		return min(source, comparator);
	}

	/**
	 * Computes the minimum value of items in the source.
	 *
	 * @param source the source containing items to compare
	 * @param comparator the comparator used to compare the items in source
	 *
	 * @return {@link Mono} of the minimum value in source
	 */
	public static <T> Mono<T> min(Publisher<T> source, Comparator<? super T> comparator) {
		return MathMono.onAssembly(new MonoMinMax<T>(source, comparator, -1));
	}

	static <T extends Comparable<? super T>> Comparator<? super T> comparableComparator() {
		return new Comparator<T>() {
			@Override
			public int compare(T o1, T o2) {
				return o1.compareTo(o2);
			}
		};
	}

	static abstract class MathMono<T> extends Mono<T> {

		protected static <T> Mono<T> onAssembly(Mono<T> source) {
			return Mono.onAssembly(source);
		}
	}

}
