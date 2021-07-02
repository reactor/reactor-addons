/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.bool;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.bool.BooleanUtils.*;
import static reactor.bool.BooleanUtils.not;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Greg Turnquist
 */
public class BooleanUtilsTest {

	private static final Mono<Boolean> TRUE = Mono.just(true);
	private static final Mono<Boolean> FALSE = Mono.just(false);

	@Test
	public void testNot() {
		StepVerifier.create(not(TRUE))
			.assertNext(bool -> assertThat(bool).as("not(TRUE)").isFalse())
			.verifyComplete();
		
		StepVerifier.create(not(FALSE))
			.assertNext(bool -> assertThat(bool).as("not(FALSE)").isTrue())
			.verifyComplete();
	}

	@Test
	public void testAnd() {
		StepVerifier.create(and(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("and(TRUE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(and(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("and(TRUE, FALSE)").isFalse())
			.verifyComplete();

		StepVerifier.create(and(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("and(FALSE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(and(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("and(FALSE, FALSE)").isFalse())
			.verifyComplete();
	}

	@Test
	public void testOr() {
		StepVerifier.create(or(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("or(TRUE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(or(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("or(TRUE, FALSE)").isTrue())
			.verifyComplete();

		StepVerifier.create(or(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("or(FALSE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(or(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("or(FALSE, FALSE)").isFalse())
			.verifyComplete();
	}

	@Test
	public void testNand() {
		StepVerifier.create(nand(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nand(TRUE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nand(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nand(TRUE, FALSE)").isTrue())
			.verifyComplete();

		StepVerifier.create(nand(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nand(FALSE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(nand(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nand(FALSE, FALSE)").isTrue())
			.verifyComplete();
	}

	@Test
	public void testNor() {
		StepVerifier.create(nor(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nor(TRUE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nor(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nor(TRUE, FALSE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nor(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("nor(FALSE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(nor(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("nor(FALSE, FALSE)").isTrue())
			.verifyComplete();
	}

	@Test
	public void testXor() {
		StepVerifier.create(xor(TRUE, TRUE))
			.assertNext(bool -> assertThat(bool).as("xor(TRUE, TRUE)").isFalse())
			.verifyComplete();

		StepVerifier.create(xor(TRUE, FALSE))
			.assertNext(bool -> assertThat(bool).as("xor(TRUE, FALSE)").isTrue())
			.verifyComplete();

		StepVerifier.create(xor(FALSE, TRUE))
			.assertNext(bool -> assertThat(bool).as("xor(FALSE, TRUE)").isTrue())
			.verifyComplete();

		StepVerifier.create(xor(FALSE, FALSE))
			.assertNext(bool -> assertThat(bool).as("xor(FALSE, FALSE)").isFalse())
			.verifyComplete();
	}
}
