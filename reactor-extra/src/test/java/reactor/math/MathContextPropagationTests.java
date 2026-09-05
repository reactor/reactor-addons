/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

@RunWith(Parameterized.class)
public class MathContextPropagationTests {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> operators() {
		return Arrays.asList(new Object[][] {
				{"sumInt", (Function<Flux<Integer>, Mono<?>>) MathFlux::sumInt, 6},
				{"sumLong", (Function<Flux<Integer>, Mono<?>>) MathFlux::sumLong, 6L},
				{"sumFloat", (Function<Flux<Integer>, Mono<?>>) MathFlux::sumFloat, 6F},
				{"sumDouble", (Function<Flux<Integer>, Mono<?>>) MathFlux::sumDouble, 6D},
				{"sumBigInteger", (Function<Flux<Integer>, Mono<?>>)
						values -> MathFlux.sumBigInteger(values, value -> BigInteger.valueOf(value.longValue())), BigInteger.valueOf(6)},
				{"sumBigDecimal", (Function<Flux<Integer>, Mono<?>>)
						values -> MathFlux.sumBigDecimal(values, value -> BigDecimal.valueOf(value.longValue())), BigDecimal.valueOf(6)},
				{"averageFloat", (Function<Flux<Integer>, Mono<?>>) MathFlux::averageFloat, 2F},
				{"averageDouble", (Function<Flux<Integer>, Mono<?>>) MathFlux::averageDouble, 2D},
				{"averageBigInteger", (Function<Flux<Integer>, Mono<?>>)
						values -> MathFlux.averageBigInteger(values, value -> BigInteger.valueOf(value.longValue())), BigInteger.valueOf(2)},
				{"averageBigDecimal", (Function<Flux<Integer>, Mono<?>>)
						values -> MathFlux.averageBigDecimal(values, value -> BigDecimal.valueOf(value.longValue())), BigDecimal.valueOf(2)},
				{"min", (Function<Flux<Integer>, Mono<?>>) MathFlux::min, 1},
				{"max", (Function<Flux<Integer>, Mono<?>>) MathFlux::max, 3}
		});
	}

	private final Function<Flux<Integer>, Mono<?>> operator;
	private final Object expected;

	public MathContextPropagationTests(String name, Function<Flux<Integer>, Mono<?>> operator, Object expected) {
		this.operator = operator;
		this.expected = expected;
	}

	@Before
	public void enableContextPropagation() {
		Hooks.onEachOperator("mathTracing", Operators.lift(
				scannable -> scannable instanceof MonoFromFluxOperator,
				(scannable, subscriber) -> subscriber));
		Hooks.enableAutomaticContextPropagation();
	}

	@After
	public void resetHooks() {
		Hooks.disableAutomaticContextPropagation();
		Hooks.resetOnEachOperator("mathTracing");
	}

	@Test
	public void mappedResult() {
		StepVerifier.create(this.operator.apply(Flux.just(1, 2, 3)).flux().map(Object::toString))
				.expectNext(this.expected.toString())
				.verifyComplete();
	}

	@Test
	public void conditionalResultWithDeferredDemand() {
		StepVerifier.create(this.operator.apply(Flux.just(1, 2, 3)).flux()
				.map(Object::toString).filter(value -> !value.isEmpty()), 0)
				.thenRequest(1)
				.expectNext(this.expected.toString())
				.verifyComplete();
	}

	@Test
	public void emptySource() {
		StepVerifier.create(this.operator.apply(Flux.empty()).flux().map(Object::toString))
				.verifyComplete();
	}

	@Test
	public void sourceError() {
		IllegalStateException failure = new IllegalStateException("source failure");
		StepVerifier.create(this.operator.apply(Flux.error(failure)).flux().map(Object::toString))
				.verifyErrorMatches(error -> error == failure);
	}

	@Test
	public void cancellation() {
		TestPublisher<Integer> source = TestPublisher.create();
		StepVerifier.create(this.operator.apply(source.flux()).flux().map(Object::toString))
				.thenCancel()
				.verify();
		source.assertCancelled();
	}
}
