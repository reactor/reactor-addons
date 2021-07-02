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

package reactor.retry;

import java.time.Duration;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultRepeatTest {

	@Test
	public void stringJustOnce() {
		Repeat test = Repeat.once();

		assertThat(test.toString())
				.isEqualTo("Repeat{times=1,backoff=Backoff{ZERO},jitter=Jitter{NONE}}");
	}

	@Test
	public void stringTwiceFixedNoJitter() {
		Repeat test = Repeat.times(2)
		                  .backoff(Backoff.fixed(Duration.ofHours(2)));

		assertThat(test.toString())
				.isEqualTo("Repeat{times=2,backoff=Backoff{fixed=7200000ms},jitter=Jitter{NONE}}");
	}

	@Test
	public void stringThreeTimesExponentialRandomJitter() {
		Backoff backoff = Backoff.exponential(
				Duration.ofMillis(12),
				Duration.ofMinutes(2),
				3,
				true);
		Repeat test = Repeat.times(3)
		                  .backoff(backoff)
		                  .jitter(Jitter.random());

		assertThat(test.toString())
				.isEqualTo("Repeat{times=3,backoff=" + backoff + ",jitter=Jitter{RANDOM-0.5}}");
	}

}