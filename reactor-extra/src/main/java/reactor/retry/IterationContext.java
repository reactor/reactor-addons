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

/**
 * Context provided to retry or repeat callbacks.
 *
 * @param <T> Application context type
 *
 * @deprecated To be removed in 3.7.0 at the earliest. Use reactor.util.repeat or
 * reactor.util.retry available since reactor-core 3.8.0 which provides similar
 * capabilities.
 */
@Deprecated
public interface IterationContext<T> {

	/**
	 * Application context that may be used to perform any rollbacks before
	 * a retry. Application context can be configured using {@link Retry#withApplicationContext(Object)}
	 * or {@link Repeat#withApplicationContext(Object)}.
	 *
	 * @return application context
	 */
	public T applicationContext();

	/**
	 * The next iteration number. This is a zero-based incrementing number with
	 * the first attempt prior to any retries as iteration zero.
	 * @return the current iteration number
	 */
	public long iteration();

	/**
	 * The backoff delay. When {@link Backoff} function is invoked, the previous
	 * backoff is provided in the context. The context provided for the retry
	 * predicates {@link Retry#onlyIf(java.util.function.Predicate)} and
	 * {@link Repeat#onlyIf(java.util.function.Predicate)} as well as the retry
	 * callbacks {@link Retry#doOnRetry(java.util.function.Consumer)} and
	 * {@link Repeat#doOnRepeat(java.util.function.Consumer)} provide the
	 * backoff delay for the next retry.
	 *
	 * @return Backoff delay
	 */
	public Duration backoff();
}
