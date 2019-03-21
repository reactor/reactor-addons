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

import java.util.Comparator;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


public class ReactorMathTests {

	@Test
	public void fluxSumInt() {
		int count = 10;
		int sum = sum(count);
		verifyResult(MathFlux.sumInt(intFlux(count)), sum);
		verifyResult(MathFlux.sumInt(Flux.just(Integer.MAX_VALUE, Integer.MAX_VALUE)), Integer.MAX_VALUE * 2);
		verifyResult(MathFlux.sumInt(shortFlux(count), i -> i), sum);
		verifyResult(MathFlux.sumInt(stringFlux(count), s -> Integer.parseInt(s)), sum);

		verifyResult(intFlux(count).as(MathFlux::sumInt), sum);
		verifyResult(intFlux(count).transform(MathFlux::sumInt), sum);
		verifyResult(doubleFlux(count).as(MathFlux::sumInt), sum);
		verifyResult(doubleFlux(count).transform(MathFlux::sumInt), sum);
	}

	@Test
	public void monoSumInt() {
		verifyResult(MathFlux.sumInt(Mono.just(5)), 5);
		verifyResult(MathFlux.sumInt(Mono.just((short) 6), i -> i), 6);
		verifyResult(MathFlux.sumInt(Mono.just("7"), s -> Integer.parseInt(s)), 7);
	}

	@Test
	public void emptySumInt() {
		verifyEmptyResult(MathFlux.sumInt(Mono.empty()));
		verifyEmptyResult(MathFlux.sumInt(Mono.<Short>empty(), i -> i));
	}

	@Test
	public void fluxSumLong() {
		int count = 10;
		long sum = sum(count);
		verifyResult(MathFlux.sumLong(longFlux(count)), sum);
		verifyResult(MathFlux.sumLong(Flux.just(Long.MAX_VALUE, Long.MAX_VALUE)), Long.MAX_VALUE * 2);
		verifyResult(MathFlux.sumLong(doubleFlux(count), i -> i), sum);
		verifyResult(MathFlux.sumLong(stringFlux(count), s -> Long.parseLong(s)), sum);

		verifyResult(longFlux(count).as(MathFlux::sumLong), sum);
		verifyResult(longFlux(count).transform(MathFlux::sumLong), sum);
		verifyResult(doubleFlux(count).as(MathFlux::sumLong), sum);
		verifyResult(doubleFlux(count).transform(MathFlux::sumLong), sum);
	}

	@Test
	public void monoSumLong() {
		verifyResult(MathFlux.sumLong(Mono.just(5L)), 5L);
		verifyResult(MathFlux.sumLong(Mono.just(1.1), i -> i), 1L);
		verifyResult(MathFlux.sumLong(Mono.just("6"), s -> Long.parseLong(s)), 6L);
	}

	@Test
	public void emptySumLong() {
		verifyEmptyResult(MathFlux.sumLong(Mono.empty()));
	}

	@Test
	public void fluxSumFloat() {
		int count = 10;
		float sum = sum(count);
		verifyResult(MathFlux.sumFloat(floatFlux(count)), sum);
		verifyResult(MathFlux.sumFloat(Flux.just(Float.MAX_VALUE, Float.MAX_VALUE)), Float.MAX_VALUE * 2);
		verifyResult(MathFlux.sumFloat(intFlux(count), i -> i), sum);
		verifyResult(MathFlux.sumFloat(stringFlux(count), s -> Float.parseFloat(s)), sum);

		verifyResult(floatFlux(count).as(MathFlux::sumFloat), sum);
		verifyResult(floatFlux(count).transform(MathFlux::sumFloat), sum);
		verifyResult(intFlux(count).as(MathFlux::sumFloat), sum);
		verifyResult(intFlux(count).transform(MathFlux::sumFloat), sum);
	}

	@Test
	public void monoSumFloat() {
		verifyResult(MathFlux.sumFloat(Mono.just(2.5F)), 2.5F);
		verifyResult(MathFlux.sumFloat(Mono.just(2), i -> i), 2F);
		verifyResult(MathFlux.sumFloat(Mono.just("6.6"), s -> Float.parseFloat(s)), 6.6F);
	}

	@Test
	public void emptySumFloat() {
		verifyEmptyResult(MathFlux.sumFloat(Mono.empty()));
	}

	@Test
	public void fluxSumDouble() {
		int count = 10;
		double sum = sum(count);
		verifyResult(MathFlux.sumDouble(doubleFlux(count)), sum);
		verifyResult(MathFlux.sumDouble(Flux.just(Double.MAX_VALUE, Double.MAX_VALUE)), Double.MAX_VALUE * 2);
		verifyResult(MathFlux.sumDouble(intFlux(count), i -> i), sum);
		verifyResult(MathFlux.sumDouble(stringFlux(count), s -> Double.parseDouble(s)), sum);

		verifyResult(doubleFlux(count).as(MathFlux::sumDouble), sum);
		verifyResult(doubleFlux(count).transform(MathFlux::sumDouble), sum);
		verifyResult(intFlux(count).as(MathFlux::sumDouble), sum);
		verifyResult(intFlux(count).transform(MathFlux::sumDouble), sum);
	}

	@Test
	public void monoSumDouble() {
		verifyResult(MathFlux.sumDouble(Mono.just(2.5)), 2.5);
		verifyResult(MathFlux.sumDouble(Mono.just(2), i -> i), 2.0);
		verifyResult(MathFlux.sumDouble(Mono.just("6.6"), s -> Double.parseDouble(s)), 6.6);
	}

	@Test
	public void emptySumDouble() {
		verifyEmptyResult(MathFlux.sumDouble(Mono.empty()));
	}

	@Test
	public void fluxAverageFloat() {
		int count = 10;
		float average = (float) average(count);
		verifyResult(MathFlux.averageFloat(floatFlux(count)), average);
		verifyResult(MathFlux.averageFloat(Flux.just(Float.MAX_VALUE, Float.MAX_VALUE)), (Float.MAX_VALUE * 2) / 2);
		verifyResult(MathFlux.averageFloat(intFlux(count), i -> i), (float) average(count));
		verifyResult(MathFlux.averageFloat(stringFlux(count), s -> Float.parseFloat(s)), (float) average(count));

		verifyResult(floatFlux(count).as(MathFlux::averageFloat), average);
		verifyResult(floatFlux(count).transform(MathFlux::averageFloat), average);
		verifyResult(intFlux(count).as(MathFlux::averageFloat), average);
		verifyResult(intFlux(count).transform(MathFlux::averageFloat), average);
	}

	@Test
	public void monoAverageFloat() {
		verifyResult(MathFlux.averageFloat(Mono.just(2.5F)), 2.5F);
		verifyResult(MathFlux.averageFloat(Mono.just(2), i -> i), 2.0F);
		verifyResult(MathFlux.averageFloat(Mono.just("1.23"), s -> Float.parseFloat(s)), 1.23F);
	}

	@Test
	public void emptyAverageFloat() {
		verifyEmptyResult(MathFlux.averageFloat(Mono.empty()));
	}

	@Test
	public void fluxAverageDouble() {
		int count = 10;
		double average = average(count);
		verifyResult(MathFlux.averageDouble(doubleFlux(count)), average);
		verifyResult(MathFlux.averageDouble(Flux.just(Double.MAX_VALUE, Double.MAX_VALUE)), (Double.MAX_VALUE * 2) / 2);
		verifyResult(MathFlux.averageDouble(intFlux(count), i -> i), average(count));
		verifyResult(MathFlux.averageDouble(stringFlux(count), s -> Double.parseDouble(s)), average(count));

		verifyResult(doubleFlux(count).as(MathFlux::averageDouble), average);
		verifyResult(doubleFlux(count).transform(MathFlux::averageDouble), average);
		verifyResult(intFlux(count).as(MathFlux::averageDouble), average);
		verifyResult(intFlux(count).transform(MathFlux::averageDouble), average);
	}

	@Test
	public void monoAverageDouble() {
		verifyResult(MathFlux.averageDouble(Mono.just(2.5)), 2.5);
		verifyResult(MathFlux.averageDouble(Mono.just(2), i -> i), 2.0);
		verifyResult(MathFlux.averageDouble(Mono.just("1.23"), s -> Double.parseDouble(s)), 1.23);
	}

	@Test
	public void emptyAverageDouble() {
		verifyEmptyResult(MathFlux.averageDouble(Mono.empty()));
	}

	@Test
	public void fluxMax() {
		verifyResult(MathFlux.max(Flux.just(5, 1, 2, 6, 3)), 6);
		verifyResult(MathFlux.max(Flux.range(1, 10)), 10);
		verifyResult(MathFlux.max(Flux.just(-1, -2, -3, -4)), -1);
		verifyResult(MathFlux.max(Flux.just("12345", "6", "78", "012"), new StringLengthComparator()), "12345");
		verifyResult(Flux.range(1, 10).as(MathFlux::max), 10);
		verifyResult(Flux.range(1, 10).transform(MathFlux::max), 10);
	}

	@Test
	public void monoMax() {
		verifyResult(MathFlux.max(Mono.just(2.5)), 2.5);
		verifyResult(MathFlux.max(Mono.just("123"), new StringLengthComparator()), "123");
	}

	@Test
	public void emptyMax() {
		verifyEmptyResult(MathFlux.max(Mono.<Integer>empty()));
	}

	@Test
	public void fluxMin() {
		verifyResult(MathFlux.min(Flux.just(5, 1, 2, 6, 3)), 1);
		verifyResult(MathFlux.min(Flux.range(1, 10)), 1);
		verifyResult(MathFlux.min(Flux.just(-1, -2, -3, -4)), -4);
		verifyResult(MathFlux.min(Flux.just("12345", "6", "78", "012"), new StringLengthComparator()), "6");
		verifyResult(Flux.range(1, 10).as(MathFlux::min), 1);
		verifyResult(Flux.range(1, 10).transform(MathFlux::min), 1);
	}

	@Test
	public void monoMin() {
		verifyResult(MathFlux.min(Mono.just(2.5)), 2.5);
		verifyResult(MathFlux.min(Mono.just("123"), new StringLengthComparator()), "123");
	}

	@Test
	public void emptyMin() {
		verifyEmptyResult(MathFlux.min(Mono.<String>empty()));
	}

	Flux<Integer> intFlux(int count) {
		return Flux.range(1, count);
	}

	Flux<Short> shortFlux(int count) {
		return Flux.range(1, count).map(i -> (short) i.intValue());
	}

	Flux<Long> longFlux(int count) {
		return Flux.range(1, count).map(i -> (long) i.intValue());
	}

	Flux<Float> floatFlux(int count) {
		return Flux.range(1, count).map(i -> (float) i.intValue());
	}

	Flux<Double> doubleFlux(int count) {
		return Flux.range(1, count).map(i -> (double) i.intValue());
	}

	Flux<String> stringFlux(int count) {
		return Flux.range(1, count).map(i -> i.toString());
	}

	int sum(int n) {
		return n * (n + 1) / 2;
	}

	double average(int n) {
		return (double) sum(n) / n;
	}

	@SuppressWarnings("unchecked")
	<T> void verifyResult(Publisher<T> resultMono, T expectedResult) {
		StepVerifier.create(resultMono)
					.expectNext(expectedResult)
					.expectComplete()
					.verify();
	}

	<T> void verifyEmptyResult(Publisher<T> resultMono) {
		StepVerifier.create(resultMono)
					.expectComplete()
					.verify();
	}

	static class StringLengthComparator implements Comparator<String> {

		@Override
		public int compare(String o1, String o2) {
			return o1.length() - o2.length();
		}

	}
}
