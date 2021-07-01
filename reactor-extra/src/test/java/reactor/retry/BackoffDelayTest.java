/*
 * Copyright (c) 2017 VMware Inc. or its affiliates, All Rights Reserved.
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

/**
 * @author Simon Baslé
 */
public class BackoffDelayTest {

	@Test
	public void toStringZero() {
		assertThat(BackoffDelay.ZERO.toString())
				.isEqualTo("{ZERO}");
	}

	@Test
	public void toStringExhausted() {
		assertThat(AbstractRetry.RETRY_EXHAUSTED.toString())
				.isEqualTo("{EXHAUSTED}");
	}

	@Test
	public void toStringSimple() {
		assertThat(new BackoffDelay(Duration.ofSeconds(3)).toString())
				.isEqualTo("{3000ms}");
	}

	@Test
	public void toStringMinMaxDelay() {
		assertThat(new BackoffDelay(
				Duration.ofSeconds(3),
				Duration.ofSeconds(4),
				Duration.ofMillis(123)
		).toString())
				.isEqualTo("{123ms/4000ms}");
	}

}