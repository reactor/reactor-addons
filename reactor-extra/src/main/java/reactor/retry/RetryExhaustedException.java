/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.retry;

/**
 * Exception indicating that retries have been exhausted after
 * {@link Retry#timeout(java.time.Duration)} or {@link Retry#retryMax(int)}.
 * For retries, {@link #getCause()} returns the original exception from the
 * last retry attempt that generated this exception.
 */
public class RetryExhaustedException extends RuntimeException {

	private static final long serialVersionUID = 6961442923363481283L;

	public RetryExhaustedException() {
		super();
	}

	public RetryExhaustedException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RetryExhaustedException(String message, Throwable cause) {
		super(message, cause);
	}

	public RetryExhaustedException(String message) {
		super(message);
	}

	public RetryExhaustedException(Throwable cause) {
		super(cause);
	}
}
