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

package reactor.retry;

import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.Function;

/**
 * Backoff function
 *
 */
public interface Backoff extends Function<IterationContext<?>, BackoffDelay> {

	Backoff ZERO_BACKOFF = new Backoff() {
		@Override
		public BackoffDelay apply(IterationContext<?> context) {
			return BackoffDelay.ZERO;
		}

		@Override
		public String toString() {
			return "Backoff{ZERO}";
		}
	};

	/**
	 * Backoff function with no backoff delay
	 * @return Backoff function for zero backoff delay
	 */
	static Backoff zero() {
		return ZERO_BACKOFF;
	}

	/**
	 * Backoff function with fixed backoff delay
	 * @param backoffInterval backoff interval
	 * @return Backoff function with fixed backoff delay
	 */
	static Backoff fixed(final Duration backoffInterval) {
		return new Backoff() {
			@Override
			public BackoffDelay apply(IterationContext<?> context) {
				return new BackoffDelay(backoffInterval);
			}

			@Override
			public String toString() {
				return "Backoff{fixed=" + backoffInterval.toMillis() + "ms}";
			}
		};
	}

	/**
	 * Backoff function with exponential backoff delay. Retries are performed after a backoff
	 * interval of <code>firstBackoff * (factor ** n)</code> where n is the iteration. If
	 * <code>maxBackoff</code> is not null, the maximum backoff applied will be limited to
	 * <code>maxBackoff</code>.
	 * <p>
	 * If <code>basedOnPreviousValue</code> is true, backoff will be calculated using
	 * <code>prevBackoff * factor</code>. When backoffs are combined with {@link Jitter}, this
	 * value will be different from the actual exponential value for the iteration.
	 *
	 * @param firstBackoff First backoff duration
	 * @param maxBackoff Maximum backoff duration
	 * @param factor The multiplicand for calculating backoff
	 * @param basedOnPreviousValue If true, calculation is based on previous value which may
	 *        be a backoff with jitter applied
	 * @return Backoff function with exponential delay
	 */
	static Backoff exponential(Duration firstBackoff, @Nullable Duration maxBackoff, int factor, boolean basedOnPreviousValue) {
		if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero())
			throw new IllegalArgumentException("firstBackoff must be > 0");
		Duration maxBackoffInterval = maxBackoff != null ? maxBackoff : Duration.ofSeconds(Long.MAX_VALUE);
		if (maxBackoffInterval.compareTo(firstBackoff) < 0)
			throw new IllegalArgumentException("maxBackoff must be >= firstBackoff");
		if (!basedOnPreviousValue) {
			return new Backoff() {
				@Override
				public BackoffDelay apply(IterationContext<?> context) {
					Duration nextBackoff;
					if (context.backoff() != null && context.backoff().compareTo(maxBackoffInterval) >= 0) {
						nextBackoff = maxBackoffInterval;
					}
					else {
						try {
							nextBackoff = firstBackoff.multipliedBy((long) Math.pow(factor, (context.iteration() - 1)));
						}
						catch (ArithmeticException e) {
							nextBackoff = maxBackoffInterval;
						}
					}
					return new BackoffDelay(firstBackoff, maxBackoffInterval, nextBackoff);
				}

				@Override
				public String toString() {
					return String.format("Backoff{exponential,min=%sms,max=%s,factor=%s,basedOnPreviousValue=false}",
							firstBackoff.toMillis(),
							maxBackoff == null ? "NONE" : maxBackoff.toMillis() + "ms",
							factor);
				}
			};
		}
		else {
			return new Backoff() {
				@Override
				public BackoffDelay apply(IterationContext<?> context) {
					Duration prevBackoff = context.backoff() == null ? Duration.ZERO : context.backoff();
					Duration nextBackoff;
					if (prevBackoff.compareTo(maxBackoffInterval) >= 0) {
						nextBackoff = maxBackoffInterval;
					}
					else try {
						nextBackoff = prevBackoff.multipliedBy(factor);
					}
					catch (ArithmeticException e) {
						nextBackoff = maxBackoffInterval;
					}
					nextBackoff = nextBackoff.compareTo(firstBackoff) < 0 ? firstBackoff : nextBackoff;
					return new BackoffDelay(firstBackoff, maxBackoff, nextBackoff);
				}

				@Override
				public String toString() {
					return String.format("Backoff{exponential,min=%sms,max=%s,factor=%s,basedOnPreviousValue=true}",
							firstBackoff.toMillis(),
							maxBackoff == null ? "NONE" : maxBackoff.toMillis() + "ms",
							factor);
				}
			};
		}
	}
}
