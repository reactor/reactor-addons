/*
 * Copyright (c) 2018-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.retry;

import java.time.Duration;

import org.junit.Test;
import org.quicktheories.QuickTheory;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

@Deprecated
public class RandomJitterTest {

	@Test
	public void negativeFactorRejected() {
		assertThatIllegalArgumentException().isThrownBy(() -> new RandomJitter(-0.1))
		                                    .withMessage("random factor must be between 0 and 1 (default 0.5)");
	}

	@Test
	public void overOneFactorRejected() {
		assertThatIllegalArgumentException().isThrownBy(() -> new RandomJitter(1.1))
		                                    .withMessage("random factor must be between 0 and 1 (default 0.5)");
	}

	@Test
	public void applyToDelayUnderMinRejected() {
		//the BackoffDelay must comply to the invariant of min <= delay <= max
		Duration min = Duration.ofMillis(100);
		Duration max = Duration.ofMillis(100);
		Duration delay = Duration.ofMillis(99);
		BackoffDelay bd = new BackoffDelay(min, max, delay);
		RandomJitter jitter = new RandomJitter(0.5);

		assertThatIllegalArgumentException().isThrownBy(() -> jitter.apply(bd))
		                                    .withMessage("jitter can only be applied on a delay that is >= to min backoff");
	}

	@Test
	public void applyToDelayOverMaxRejected() {
		//the BackoffDelay must comply to the invariant of min <= delay <= max
		Duration min = Duration.ofMillis(100);
		Duration max = Duration.ofMillis(200);
		Duration delay = Duration.ofMillis(201);
		BackoffDelay bd = new BackoffDelay(min, max, delay);
		RandomJitter jitter = new RandomJitter(0.5);

		assertThatIllegalArgumentException().isThrownBy(() -> jitter.apply(bd))
		                                    .withMessage("jitter can only be applied on a delay that is <= to max backoff");
	}

	@Test
	public void jitterOffsetCappedForSuperLargeMaxDuration() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(100);
		Duration max = Duration.ofMillis(100);
		Duration delay = Duration.ofSeconds(Long.MAX_VALUE);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		assertThat(jitter.jitterOffsetCapped(bd))
				.isEqualTo(Math.round(Long.MAX_VALUE * factor));
	}

	@Test
	public void jitterOffsetForMaximumMillisDuration() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(100);
		Duration max = Duration.ofMillis(100);
		Duration delay = Duration.ofMillis(Long.MAX_VALUE);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		long expected = delay.multipliedBy(50)
				.dividedBy(100)
				.toMillis();

		assertThat(jitter.jitterOffsetCapped(bd))
				.isEqualTo(expected);
	}

	@Test
	public void jitterOffsetForReasonableDuration() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(100);
		Duration max = Duration.ofMillis(100);
		Duration delay = Duration.ofMillis(500);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		assertThat(jitter.jitterOffsetCapped(bd))
				.isEqualTo(250);
	}

	@Test
	public void lowBoundNormal() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(100); //not over half the duration
		Duration max = Duration.ofMillis(-123); //should be ignored
		Duration delay = Duration.ofMillis(500);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		assertThat(jitter.lowJitterBound(bd, 250))
				.isEqualTo(-250);
	}

	@Test
	public void lowBoundFlooredAtMin() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(300); //over half the duration
		Duration max = Duration.ofMillis(-123); //should be ignored
		Duration delay = Duration.ofMillis(500);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		assertThat(jitter.lowJitterBound(bd, 250))
				.as("offset over min")
				.isEqualTo(-200);

		assertThat(jitter.lowJitterBound(bd, Long.MAX_VALUE / 2))
				.as("offset half long max")
				.isEqualTo(-200);

		assertThat(jitter.lowJitterBound(bd, Long.MAX_VALUE))
				.as("offset long max")
				.isEqualTo(-200);
	}

	@Test
	public void highBoundNormal() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(-123); //should be ignored
		Duration max = Duration.ofMillis(1000); //not under half the duration
		Duration delay = Duration.ofMillis(500);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		assertThat(jitter.highJitterBound(bd, 250))
				.isEqualTo(250);
	}

	@Test
	public void highBoundCappedAtMax() {
		double factor = 0.5;
		Duration min = Duration.ofMillis(-123); //should be ignored
		Duration max = Duration.ofMillis(550); //under half the duration
		Duration delay = Duration.ofMillis(500);
		BackoffDelay bd = new BackoffDelay(min, max, delay);

		RandomJitter jitter = new RandomJitter(factor);

		assertThat(jitter.highJitterBound(bd, 250))
				.as("offset over min")
				.isEqualTo(50);

		assertThat(jitter.highJitterBound(bd, Long.MAX_VALUE / 2))
				.as("offset half long max")
				.isEqualTo(50);

		assertThat(jitter.highJitterBound(bd, Long.MAX_VALUE))
				.as("offset long max")
				.isEqualTo(50);
	}

	@Test
	public void propertyJitterDoesntCrossMinMax() {
		Gen<Long> minGen = SourceDSL.longs()
		                            .between(0, Long.MAX_VALUE - 2);

		Gen<Long> delayGen = minGen.flatMap(d -> SourceDSL.longs().between(d, Long.MAX_VALUE - 1));
		Gen<Long> maxGen = delayGen.flatMap(d -> SourceDSL.longs().between(d, Long.MAX_VALUE));

		QuickTheory.qt()
		           .withGenerateAttempts(1000)
		           .forAll(minGen, delayGen, maxGen)
		           .assuming((min, d, max) -> min <= d && d <= max)
		           .as((min, d, max) -> new BackoffDelay(Duration.ofMillis(min), Duration.ofMillis(max), Duration.ofMillis(d)))
		           .checkAssert(bd -> assertThat(Jitter.random().apply(bd))
				           .isGreaterThanOrEqualTo(bd.min)
				           .isLessThanOrEqualTo(bd.max));
	}

}