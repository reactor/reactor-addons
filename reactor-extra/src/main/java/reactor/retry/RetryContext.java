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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Class that encapsulates the retry context. The context is provided
 * retry callbacks configured using {@link RetryBuilder#doOnRetry(java.util.function.Consumer)}.
 * Application context may be included in the <code>RetryContext<T></code> for
 * easy access to application state in any rollback operations.
 *
 */
public class RetryContext<T> implements Cloneable {

	final T applicationContext;
	long attempts;
	Throwable exception;
	Long companionValue;
	Duration backoff;
	Duration minBackoff;
	Duration maxBackoff;

	/**
	 * Creates a retry context with the specified application context which may be
	 * used to perform any rollbacks using {@link RetryBuilder#doOnRetry(java.util.function.Consumer)}
	 * and to make retry decisions using {@link RetryBuilder#onlyIf(java.util.function.Predicate)}.
	 *
	 * @param applicationContext Context provided by application
	 */
	public RetryContext(T applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * Returns the number of attempts performed so far.
	 * @return number of previous attempts
	 */
	public long getAttempts() {
		return attempts;
	}

	RetryContext<T> setAttempts(long attempts) {
		this.attempts = attempts;
		return this;
	}

	/**
	 * Returns any exception in the last attempt.
	 * @return exception if performing retry, null if performing repeat
	 */
	public Throwable getException() {
		return exception;
	}

	RetryContext<T> setException(Throwable exception) {
		this.exception = exception;
		return this;
	}

	/**
	 * Returns the value provided in the companion Flux for repeats.
	 * <ul>
	 *   <li>For {@link Flux#retryWhen(java.util.function.Function)} and {@link Mono#retryWhen(java.util.function.Function)},
	 *      value is set to null and the exception is returned by {@link #getException()}.</li>
	 *   <li>For {@link Flux#repeatWhen(java.util.function.Function)} and {@link Mono#repeatWhen(java.util.function.Function)},
	 *      value is the number of items emitted in the last attempt.
	 *   <li>For {@link Mono#repeatWhenEmpty(java.util.function.Function)} and {@link Mono#repeatWhenEmpty(int, java.util.function.Function)},
	 *      value is a zero-based incrementing Long, which is {@link #getAttempts()} - 1.
	 * </ul>
	 * @return value the value emitted on the companion Flux for repeats.
	 */
	public Long getCompanionValue() {
		return companionValue;
	}

	RetryContext<T> setCompanionValue(Long companionValue) {
		this.companionValue = companionValue;
		return this;
	}

	/**
	 * Returns any backoff delay before another attempt may be performed.
	 * @return backoff delay before next attempt
	 */
	public Duration getBackoff() {
		return backoff;
	}

	RetryContext<T> setBackoff(Duration backoff) {
		this.backoff = backoff;
		return this;
	}

	/**
	 * Returns the minimum backoff delay. If the backoff delay calculated is less than this
	 * value, this minimum will be used.
	 * @return minimum backoff delay
	 */
	public Duration getMinBackoff() {
		return minBackoff;
	}

	void setMinBackoff(Duration minBackoff) {
		this.minBackoff = minBackoff;
	}

	/**
	 * Returns the maximum backoff delay. If the backoff delay calculated is greater than this
	 * value, this maximum will be used.
	 * @return maximum backoff delay
	 */
	public Duration getMaxBackoff() {
		return maxBackoff;
	}

	void setMaxBackoff(Duration maxBackoff) {
		this.maxBackoff = maxBackoff;
	}

	/**
	 * Returns the application context configured on this RetryContext<T>.
	 * @return application context if configured, null otherwise
	 */
	public Object getApplicationContext() {
		return applicationContext;
	}

	@Override
	protected RetryContext<T> clone() {
		RetryContext<T> clone = new RetryContext<T>(applicationContext);
		clone.attempts = attempts;
		clone.exception = exception;
		clone.companionValue = companionValue;
		clone.backoff = backoff;
		return clone;
	}

	@Override
	public String toString() {
		return String.format("attempts=%d exception=%s value=%d backoff=%s", attempts, exception, companionValue, backoff);
	}
}