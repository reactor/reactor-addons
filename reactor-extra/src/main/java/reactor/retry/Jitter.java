/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
 */
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

	Jitter RANDOM_JITTER = new Jitter() {
		@Override
		public Duration apply(BackoffDelay backoff) {
			ThreadLocalRandom random = ThreadLocalRandom.current();
			long backoffMs = backoff.delay().toMillis();
			long minBackoffMs = backoff.min.toMillis();
			long jitterBackoffMs = backoffMs == minBackoffMs ? minBackoffMs : random.nextLong(minBackoffMs, backoffMs);
			return Duration.ofMillis(jitterBackoffMs);
		}

		@Override
		public String toString() {
			return "Jitter{RANDOM}";
		}
	};

	/**
	 * Jitter function that is a no-op.
	 * @return Jitter function that does not apply any jitter
	 */
	static Jitter noJitter() {
		return NO_JITTER;
	}

	/**
	 * Jitter function that applies a random jitter to choose a random backoff
	 * delay between {@link BackoffDelay#minDelay()} and {@link BackoffDelay#delay()}.
	 * @return Jitter function to randomize backoff delay
	 */
	static Jitter random() {
		return RANDOM_JITTER;
	}
}
