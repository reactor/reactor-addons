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

package reactor.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Jitter function that is applied to the backoff delay.
 *
 * @deprecated Use reactor.util.repeat or reactor.util.retry available since
 * reactor-core 3.8.0 which provides similar capabilities.
 */
@Deprecated
public interface Jitter extends Function<BackoffDelay, Duration> {

	Jitter NO_JITTER = new Jitter() {
		@Override
		public Duration apply(BackoffDelay delay) {
			return delay.delay();
		}

		@Override
		public String toString() {
			return "Jitter{NONE}";
		}
	};

	Jitter RANDOM_JITTER = new RandomJitter(0.5);

	/**
	 * Jitter function that is a no-op.
	 * @return Jitter function that does not apply any jitter
	 */
	static Jitter noJitter() {
		return NO_JITTER;
	}

	/**
	 * Jitter function that applies a random jitter with a factor of 0.5, generating a
	 * backoff between {@code [d - d*0.5; d + d*0.5]} (but still within the limits of
	 * [{@link BackoffDelay#minDelay()}; {@link BackoffDelay#maxDelay()}].
	 * @return Jitter function to randomize backoff delay
	 */
	static Jitter random() {
		return RANDOM_JITTER;
	}

	/**
	 * Jitter function that applies a random jitter with a provided [0; 1] factor (default 0.5),
	 * generating a backoff between {@code [d - d*factor; d + d*factor]} (but still within
	 * the limits of [{@link BackoffDelay#minDelay()}; {@link BackoffDelay#maxDelay()}].
	 * @return Jitter function to randomize backoff delay
	 */
	static Jitter random(double randomFactor) {
		return new RandomJitter(randomFactor);
	}
}
