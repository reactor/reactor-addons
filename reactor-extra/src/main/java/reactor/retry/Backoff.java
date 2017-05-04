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
import java.util.function.Function;

public interface Backoff extends Function<Context<?>, BackoffDelay> {

	public static final Backoff ZERO_BACKOFF = context -> BackoffDelay.ZERO;

	static Backoff zero() {
		return ZERO_BACKOFF;
	}

	static Backoff fixed(Duration backoffInterval) {
		return context -> new BackoffDelay(backoffInterval);
	}

	static Backoff exponential(Duration firstBackoff, Duration maxBackoff, int factor, boolean basedOnPreviousValue) {
		if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero())
			throw new IllegalArgumentException("firstBackoff must be > 0");
		Duration maxBackoffInterval = maxBackoff != null ? maxBackoff : Duration.ofSeconds(Long.MAX_VALUE);
		if (maxBackoffInterval.compareTo(firstBackoff) <= 0)
			throw new IllegalArgumentException("maxBackoff must be >= firstBackoff");
		if (!basedOnPreviousValue) {
			return context -> {
				Duration nextBackoff = firstBackoff.multipliedBy((long) Math.pow(factor, (context.iteration() - 1)));
				return new BackoffDelay(firstBackoff, maxBackoffInterval, nextBackoff);
			};
		}
		else {
			return context -> {
				Duration prevBackoff = context.backoff() == null ? Duration.ZERO : context.backoff();
				Duration nextBackoff = prevBackoff.multipliedBy(factor);
				nextBackoff = nextBackoff.compareTo(firstBackoff) < 0 ? firstBackoff : nextBackoff;
				return new BackoffDelay(firstBackoff, maxBackoff, nextBackoff);
			};
		}
	}

}
