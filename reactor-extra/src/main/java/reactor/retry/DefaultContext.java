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

public class DefaultContext<T> implements RetryContext<T>, RepeatContext<T> {

	final T applicationContext;
	final long iteration;
	final Long repeatCompanionValue;
	final Throwable exception;
	final BackoffDelay backoff;
	BackoffDelay lastBackoff;

	public DefaultContext(T applicationContext,
			long iteration,
			BackoffDelay backoff,
			long repeatCompanionValue) {
		this(applicationContext, iteration, backoff, repeatCompanionValue, null);
	}

	public DefaultContext(T applicationContext,
			long iteration,
			BackoffDelay backoff,
			Throwable exception) {
		this(applicationContext, iteration, backoff, null, exception);
	}

	private DefaultContext(T applicationContext,
			long iteration,
			BackoffDelay backoff,
			Long repeatCompanionValue,
			Throwable exception) {
		this.applicationContext = applicationContext;
		this.iteration = iteration;
		this.backoff = backoff;
		this.repeatCompanionValue = repeatCompanionValue;
		this.exception = exception;
	}

	public T applicationContext() {
		return applicationContext;
	}

	public long iteration() {
		return iteration;
	}

	public Long companionValue() {
		return repeatCompanionValue;
	}

	public Throwable exception() {
		return exception;
	}

	public Duration backoff() {
		return backoff == null ? null : backoff.delay;
	}

	@Override
	public String toString() {
		if (exception != null)
			return String.format("iteration=%d exception=%s backoff=%s", iteration, exception, backoff);
		else
			return String.format("iteration=%d repeatCompanionValue=%s backoff=%s", iteration, repeatCompanionValue, backoff);
	}
}
